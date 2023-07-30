package jsondatabase.client

import jsondatabase.server.PORT
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import java.net.InetAddress
import java.net.Socket

const val REQUEST_FILE_PATH = "/JSON Database (Kotlin)/task/src/jsondatabase/client/data/"

class Client(private val ipAddress: String, private val port: Int) {
    val socket = Socket(InetAddress.getByName(this.ipAddress), this.port)
    val input = DataInputStream(this.socket.getInputStream())
    val output = DataOutputStream(this.socket.getOutputStream())
}

fun main(args: Array<String>) {
    val cmdJson: String
    if (args.contains("-in") && args.lastIndex > args.indexOf("-in")) {
        val wd = System.getProperty("user.dir")
        val requestFile = File(wd + REQUEST_FILE_PATH + args[args.indexOf("-in") + 1])
        cmdJson = requestFile.readText()
    } else {
        var cmd = "{ "
        if (args.contains("-t") && args.lastIndex > args.indexOf("-t")) cmd += "\"type\": \"${args[args.indexOf("-t") + 1]}\", "
        if (args.contains("-k") && args.lastIndex > args.indexOf("-k")) cmd += "\"key\": \"${args[args.indexOf("-k") + 1]}\", "
        if (args.contains("-v") && args.lastIndex > args.indexOf("-v")) cmd += "\"value\": \"${args[args.indexOf("-v") + 1]}\", "
        cmdJson = "${cmd.dropLast(minOf(2, cmd.length - 1))} }"
    }
    val client = Client("127.0.0.1", PORT)
    println("Client started!")
    println("Sent: $cmdJson")
    client.output.writeUTF(cmdJson)
    println("Received: ${client.input.readUTF()}")
    client.socket.close()
}