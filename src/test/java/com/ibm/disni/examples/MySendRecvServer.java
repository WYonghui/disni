package com.ibm.disni.examples;

import com.ibm.disni.CmdLineCommon;
import com.ibm.disni.rdma.RdmaActiveEndpoint;
import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.RdmaEndpointFactory;
import com.ibm.disni.rdma.RdmaServerEndpoint;
import com.ibm.disni.rdma.verbs.*;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * java -cp disni-1.7-jar-with-dependencies.jar:disni-1.7-tests.jar com.ibm.disni.examples.MySendRecvServer -a 10.10.0.97 -p 11919
 */
public class MySendRecvServer implements RdmaEndpointFactory<MySendRecvServer.ServerEndpoint> {
    private RdmaActiveEndpointGroup<ServerEndpoint> endpointGroup;
    private String host;
    private int port;

    @Override
    public ServerEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new ServerEndpoint(endpointGroup, id, serverSide);
    }

    public void run() throws Exception{
        endpointGroup = new RdmaActiveEndpointGroup<ServerEndpoint>(1000, false, 128, 10, 128);
        endpointGroup.init(this);
        //创建一个endpoint Server
        RdmaServerEndpoint<ServerEndpoint> endpointServer = endpointGroup.createServerEndpoint();

        //将endpoint Server绑定到指定端口
        InetAddress ipAddr = InetAddress.getByName(host);
        InetSocketAddress socketAddr = new InetSocketAddress(ipAddr, port);
        endpointServer.bind(socketAddr, 10);
        System.out.println("Server::server bounds to address: " + socketAddr.toString());

        //接受连接
        ServerEndpoint endpoint = endpointServer.accept();
        System.out.println("Server::client connection accept");

        //检查complete queue,接收来自client的消息
        IbvWC wc = endpoint.getWcList().take();
        System.out.println("Server::the id of the message is " + wc.getWr_id());
        ByteBuffer recvBuffer = endpoint.getRecvBuffer();
        recvBuffer.clear();
        System.out.println("Server:: message from the client is \'" + recvBuffer.asCharBuffer().toString() + "\'");

        //向client回复一个消息
        ByteBuffer sendBuffer = endpoint.getSendBuffer();
        sendBuffer.asCharBuffer().put("Hello, client. This is server.");
        sendBuffer.clear();

        endpoint.postSend(endpoint.getWrList_send()).execute().free();
        endpoint.getWcList().take();
        System.out.println("Server::message send");

        //close all things
        System.out.println("Server::close server");
        endpoint.close();
        endpointServer.close();
        endpointGroup.close();
    }

    public void launch(String[] args) throws Exception{
        CmdLineCommon cmd = new CmdLineCommon("SendRecvServer");

        try {
            cmd.parse(args);
        } catch (ParseException e){
            cmd.printHelp();
            System.exit(-1);
        }

        this.host = cmd.getIp();
        this.port = cmd.getPort();

        this.run();
    }

    public static void main(String[] args) throws Exception{
        MySendRecvServer server = new MySendRecvServer();
        server.launch(args);
    }

    /**
     * 1. 定义endpoint
     */
    public static class ServerEndpoint extends RdmaActiveEndpoint {
        private ByteBuffer[] buffers;
        private IbvMr[] mrs;
        private int bufferCount;
        private int bufferSize;

        //申请发送缓冲区和接收缓冲区
        private ByteBuffer sendBuffer;
        private IbvMr sendMr;
        private ByteBuffer recvBuffer;
        private IbvMr recvMr;

        private IbvSge sendSge;
        private LinkedList<IbvSge> sgeList_send;
        private IbvSendWR sendWR;
        private LinkedList<IbvSendWR> wrList_send;

        private IbvSge recvSge;
        private LinkedList<IbvSge> sgeList_recv;
        private IbvRecvWR recvWR;
        private LinkedList<IbvRecvWR> wrList_recv;

        private ArrayBlockingQueue<IbvWC> wcList;

        public ServerEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
            super(group, idPriv, serverSide);

            bufferCount = 2;
            bufferSize = 100;
            buffers = new ByteBuffer[bufferCount];
            mrs = new IbvMr[bufferCount];

            for (int i = 0; i < bufferCount; i++) {
                buffers[i] = ByteBuffer.allocateDirect(bufferSize);
            }

            sendSge = new IbvSge();
            sgeList_send = new LinkedList<>();
            sendWR = new IbvSendWR();
            wrList_send = new LinkedList<>();

            recvSge = new IbvSge();
            sgeList_recv = new LinkedList<>();
            recvWR = new IbvRecvWR();
            wrList_recv = new LinkedList<>();

            wcList = new ArrayBlockingQueue<IbvWC>(10);

        }

        //重写init方法，准备缓冲区。同时，保证在连接建立时，至少执行一个读请求
        @Override
        protected synchronized void init() throws IOException {
            super.init();

            //memory region通过注册获得
            for (int i = 0; i < bufferCount; i++) {
                mrs[i] = this.registerMemory(buffers[i]).execute().getMr();
            }

            sendBuffer = buffers[0];
            sendMr = mrs[0];
            recvBuffer = buffers[1];
            recvMr = mrs[1];

            sendSge.setAddr(sendMr.getAddr());
            sendSge.setLength(sendMr.getLength());
            sendSge.setLkey(sendMr.getLkey());
            sgeList_send.add(sendSge);
            sendWR.setWr_id(1999);
            sendWR.setSg_list(sgeList_send);
            sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
            sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
            wrList_send.add(sendWR);

            //执行recv操作需要准备a descriptor
            recvSge.setAddr(recvMr.getAddr());
            recvSge.setLength(recvMr.getLength());
            recvSge.setLkey(recvMr.getLkey());
            sgeList_recv.add(recvSge);
            recvWR.setSg_list(sgeList_recv);
            recvWR.setWr_id(2000);
            wrList_recv.add(recvWR);

            System.out.println("Server::initiated receive");
            this.postRecv(wrList_recv).execute().free();
        }

        @Override
        public void dispatchCqEvent(IbvWC wc) throws IOException {
            wcList.add(wc);
        }

        public ByteBuffer getSendBuffer() {
            return sendBuffer;
        }

        public LinkedList<IbvSendWR> getWrList_send() {
            return wrList_send;
        }

        public ByteBuffer getRecvBuffer() {
            return recvBuffer;
        }

        public ArrayBlockingQueue<IbvWC> getWcList() {
            return wcList;
        }
    }
}
