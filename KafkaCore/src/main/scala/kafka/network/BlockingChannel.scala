/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.net.InetSocketAddress
import java.nio.channels._
import kafka.utils.{nonthreadsafe, Logging}
import kafka.api.RequestOrResponse
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.data.ACL
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider
import java.util.ArrayList
import kafka.utils.ZkUtils
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.Id
import kafka.javaapi.TopicMetadataRequest
import kafka.utils.Utils


object BlockingChannel{
  val UseDefaultBufferSize = -1
}

/**
 *  A simple blocking channel with timeouts correctly enabled.
 *
 */
@nonthreadsafe
class BlockingChannel( val host: String, 
                       val port: Int, 
                       val readBufferSize: Int, 
                       val writeBufferSize: Int, 
                       val readTimeoutMs: Int ) extends Logging {
  private var connected = false
  private var channel: SocketChannel = null
  private var readChannel: ReadableByteChannel = null
  private var writeChannel: GatheringByteChannel = null
  private val lock = new Object()
  private val connectTimeoutMs = readTimeoutMs

  def connect() = lock synchronized  {
    if(!connected) {
      try {
        channel = SocketChannel.open()
        if(readBufferSize > 0)
          channel.socket.setReceiveBufferSize(readBufferSize)
        if(writeBufferSize > 0)
          channel.socket.setSendBufferSize(writeBufferSize)
        channel.configureBlocking(true)
        channel.socket.setSoTimeout(readTimeoutMs)
        channel.socket.setKeepAlive(true)
        channel.socket.setTcpNoDelay(true)
        channel.socket.connect(new InetSocketAddress(host, port), connectTimeoutMs)

        writeChannel = channel
        readChannel = Channels.newChannel(channel.socket().getInputStream)
        connected = true
        // settings may not match what we requested above
        val msg = "Created socket with SO_TIMEOUT = %d (requested %d), SO_RCVBUF = %d (requested %d), SO_SNDBUF = %d (requested %d), connectTimeoutMs = %d."
        debug(msg.format(channel.socket.getSoTimeout,
                         readTimeoutMs,
                         channel.socket.getReceiveBufferSize, 
                         readBufferSize,
                         channel.socket.getSendBufferSize,
                         writeBufferSize,
                         connectTimeoutMs))

      } catch {
        case e: Throwable => disconnect()
      }
    }
  }
  
  def disconnect() = lock synchronized {
    if(channel != null) {
      swallow(channel.close())
      swallow(channel.socket.close())
      channel = null
      writeChannel = null
    }
    // closing the main socket channel *should* close the read channel
    // but let's do it to be sure.
    if(readChannel != null) {
      swallow(readChannel.close())
      readChannel = null
    }
    connected = false
  }

  def isConnected = connected

  def send(request: RequestOrResponse):Int = {
    if(!connected)
      throw new ClosedChannelException()
    
    
    //println(props.getProperty("zookeeper.connect"))
    
    /*val send = new BoundedByteBufferSend(request)
    send.writeCompletely(writeChannel)*/
    
    val req = request.toString();
    var name = ""
    if(req.split(";")(0).split(":").size==2)
      name = req.split(";")(0).split(":")(1).trim()
    
    /*if(name.equals("TopicMetadataRequest")){
      val topic = req.split(";")(4).split(":")(1).trim()
      val aclAuth = req.split(";")(5).trim().substring(8).trim()
        
      var zkConnect = req.split(";")(6).trim().substring(10).trim()
      if(zkConnect.equals("")){

        val props = Utils.loadProps(path+"config/server.properties")
        zkConnect = props.getProperty("zookeeper.connect")
      }
      println(zkConnect)
        
      if(zkConnect.equals("")){
        println("please set correct zookeeper ")
          -1
      }
      val zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)   
      try{
        val topicpath = ZkUtils.getTopicPath(topic)
        val aclOri = zkClient.getAcl(topicpath, new Stat())
        
        val aclCreatorList = new ArrayList[ACL]()
        if(aclOri!=null&&aclOri.size()>0){
          for (a:ACL <- aclOri.asScala ){
            if(a.getId().getId().equals(DigestAuthenticationProvider.generateDigest(aclAuth))){
              aclCreatorList.add(a)
            }            
          }
        }
        
        if(aclOri.contains(Ids.OPEN_ACL_UNSAFE.get(0))){
          val send = new BoundedByteBufferSend(request)
          send.writeCompletely(writeChannel)
        }else if(aclCreatorList.size()>0){
          val send = new BoundedByteBufferSend(request)
          send.writeCompletely(writeChannel)
        }else{
          println("You do not have invalid ACL!")
          -1
        }
        
      } catch {
        case e: Throwable =>
          println("Error while executing topic command " + e.getMessage)
          -1
      } finally {
        zkClient.close()
      }
      
      
    }else */if(name.equals("OffsetRequest")){
      val topic = req.split(";")(5).split(":")(1).trim().split(",")(0).substring(1)
      val aclAuth = req.split(";")(6).trim().substring(8).trim()
        
      val zkConnect = req.split(";")(7).trim().substring(10).trim()      
      
      if(zkConnect.equals("")){
        println("please set correct zookeeper ")
          -1
      }
      
      val zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)   
      try{
        val topicpath = ZkUtils.getTopicPath(topic)
        val aclOri = zkClient.getAcl(topicpath, new Stat())
        
        val aclCreatorList = new ArrayList[ACL]()
        if(aclOri!=null&&aclOri.size()>0){
          for (a:ACL <- aclOri.asScala ){
            if(a.getId().getId().equals(DigestAuthenticationProvider.generateDigest(aclAuth))){
              aclCreatorList.add(a)
            }            
          }
        }
        if(aclOri.contains(Ids.OPEN_ACL_UNSAFE.get(0))){
          val send = new BoundedByteBufferSend(request)
          send.writeCompletely(writeChannel)
        }else if(aclCreatorList.size()>0){
          val send = new BoundedByteBufferSend(request)
          send.writeCompletely(writeChannel)
        }else{
          println("You do not have invalid ACL!")
          -1
        }
      }catch {
        case e: Throwable =>
          println("Error while executing topic command " + e.getMessage)
          -1
      } finally {
        zkClient.close()
      }
      
    }else{
      val send = new BoundedByteBufferSend(request)
      send.writeCompletely(writeChannel)
    } 
    
  }
  
  def receive(): Receive = {
    if(!connected)
      throw new ClosedChannelException()

    val response = new BoundedByteBufferReceive()
    response.readCompletely(readChannel)

    response
  }

}
