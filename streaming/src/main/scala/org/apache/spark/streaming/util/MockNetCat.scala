package org.apache.spark.streaming.util

/**
 * @author bluejoe2008@gmail.com
 */
import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.io.OutputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.io.PrintWriter
import java.net.ServerSocket

object MockNetCat {
	def start(port: Int) = new MockNetCat(port);
}

class MockNetCat(port: Int) {
	val pout = new PipedOutputStream();
	val pin = new PipedInputStream(pout);
	val serverSocket = new ServerSocket(port);

	val listenThread = new Thread() {
		override def run() {
			val socket = serverSocket.accept();
			val sout = socket.getOutputStream();
			val sin = socket.getInputStream();
			val thread1 = createSyncThread(pin, sout);
			val thread2 = createSyncThread(sin, System.out);
			thread1.start();
			thread2.start();
			thread1.join();
			socket.shutdownOutput();
			thread2.join();
		}
	};

	listenThread.start();

	def writeData(text: String) {
		pout.write(text.getBytes());
	}

	def stop() = {
		serverSocket.close();
		listenThread.stop();
	}

	private def createSyncThread(in: InputStream, out: OutputStream) = new Thread() {
		override def run() {
			val writer = new PrintWriter(out);
			val reader = new BufferedReader(new InputStreamReader(in));
			var line = "";
			do {
				line = reader.readLine();
				if (line != null) {
					writer.println(line);
					writer.flush();
				}
			} while (line != null)
		}
	}
}