package jsondatabase.server

import kotlinx.serialization.json.*
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

const val PORT = 28657
const val DATABASE_PATH = "/JSON Database (Kotlin)/task/src/jsondatabase/server/data/"
const val DATABASE_NAME = "db.json"

class Server(private val port: Int) {
    val server = ServerSocket().apply {
        reuseAddress = true
        bind(InetSocketAddress(port))
    }
    private var socket: Socket = server.accept()
    var input = DataInputStream(socket.getInputStream())
    var output = DataOutputStream(socket.getOutputStream())

    fun getNewConnection() {
        socket = server.accept()
        input = DataInputStream(socket.getInputStream())
        output = DataOutputStream(socket.getOutputStream())
    }
}

class Locks {
    private val lock: ReadWriteLock = ReentrantReadWriteLock()
    val readLock: Lock = lock.readLock()
    val writeLock: Lock = lock.writeLock()
}

enum class Commands(val action: (inp: JsonObject, dbFile: File, locks: Locks) -> String) {
    SET({ args, database, locks -> setData(args, database, locks) }),
    GET({ args, database, locks -> getData(args, database, locks) }),
    DELETE({ args, database, locks -> deleteData(args, database, locks) });

    companion object {
        fun task(cmd: JsonObject): (JsonObject, File, Locks) -> String {
            for (command in values()) {
                if (!cmd.containsKey("type")) throw Exception("No command sent")
                if (cmd["type"]!!.toString().trim('"').uppercase() == command.name) return command.action
            }
            throw Exception("No such command: ${cmd["type"]}")
        }
    }
}

fun getKeys(args: JsonObject): List<String> {
    val cmd = args.toMap()
    return try {
        cmd["key"]!!.jsonArray.map { it.toString().trim('"') }
    } catch (e: java.lang.Exception) {
        listOf(cmd["key"]!!.jsonPrimitive.toString().trim('"'))
    }
}

fun setData(args: JsonObject, dbFile: File, locks: Locks): String {
    val cmd = args.toMap()
    val keys = getKeys(args)
    locks.writeLock.lock()
    if (!dbFile.exists()) {
        dbFile.createNewFile()
        dbFile.writeText("{}")
    }
    var db = Json.parseToJsonElement(dbFile.readText()) as JsonObject
    db = setValue(db, keys, cmd["value"]!!)
    dbFile.writeText(db.toString())
    locks.writeLock.unlock()
    return "{ \"response\": \"OK\" }"
}

fun getData(args: JsonObject, dbFile: File, locks: Locks): String {
    if (!dbFile.exists()) throw Exception("No such key")
    val keys = getKeys(args)
    locks.readLock.lock()
    val db = Json.parseToJsonElement(dbFile.readText()) as JsonObject
    locks.readLock.unlock()
    return "{ \"response\": \"OK\", \"value\": ${getValue(db, keys)} }"
}

fun deleteData(args: JsonObject, dbFile: File, locks: Locks): String {
    if (!dbFile.exists()) throw Exception("No such key")
    val keys = getKeys(args)
    locks.writeLock.lock()
    var db = Json.parseToJsonElement(dbFile.readText()) as JsonObject
    db = deleteValue(db, keys)
    dbFile.writeText(db.toString())
    locks.writeLock.unlock()
    return "{ \"response\": \"OK\" }"
}

fun setValue(db: JsonObject, keys: List<String>, newValue: JsonElement): JsonObject {
    val dbMap = db.toMutableMap()
    if (keys.size == 1) {
        dbMap[keys.first()] = newValue
    } else if (dbMap.containsKey(keys.first())) {
        dbMap[keys.first()] = setValue(dbMap[keys.first()] as JsonObject, keys.drop(1), newValue)
    } else {
        dbMap[keys.first()] = Json.parseToJsonElement("{}")
        dbMap[keys.first()] = setValue(dbMap[keys.first()] as JsonObject, keys.drop(1), newValue)
    }
    return JsonObject(dbMap)
}

fun getValue(db: JsonObject, keys: List<String>): JsonElement {
    try {
        if (keys.size == 1) return db[keys.first()]!!
        return getValue(db[keys.first()] as JsonObject, keys.drop(1))
    } catch (e: java.lang.Exception) {
        throw Exception("No such key")
    }
}

fun deleteValue(db: JsonObject, keys: List<String>): JsonObject {
    val dbMap = db.toMutableMap()
    if (dbMap.containsKey(keys.first())) {
        try {
            if (keys.size == 1) {
                dbMap.remove(keys.first())
            } else {
                dbMap[keys.first()] = deleteValue(dbMap[keys.first()] as JsonObject, keys.drop(1))
            }
        } catch (e: java.lang.Exception) {
            throw Exception("No such key")
        }
    } else {
        throw Exception("No such key")
    }
    return JsonObject(dbMap)
}

fun main() {
    val locks = Locks()
    val wd = System.getProperty("user.dir")
    Files.createDirectories(Paths.get(wd + DATABASE_PATH))
    val dbFile = File(wd + DATABASE_PATH + DATABASE_NAME)
    val executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())
    println("Server started!")
    val dbServer = Server(PORT)
    var cmd = Json.parseToJsonElement(dbServer.input.readUTF()) as JsonObject
    while (cmd["type"].toString().trim('"') != "exit") {
        executor.submit {
            try {
                dbServer.output.writeUTF(Commands.task(cmd)(cmd, dbFile, locks))
            } catch (exception: Exception) {
                dbServer.output.writeUTF("{ \"response\": \"ERROR\", \"reason\": \"${exception.message}\" }")
            }
        }
        dbServer.getNewConnection()
        cmd = Json.parseToJsonElement(dbServer.input.readUTF()) as JsonObject
    }
    executor.shutdown()
    executor.awaitTermination(5, TimeUnit.SECONDS)
    dbServer.output.writeUTF("{ \"response\": \"OK\" }")
    dbServer.server.close()
}