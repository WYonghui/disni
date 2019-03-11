package com.ibm.disni.examples;

import com.ibm.disni.CmdLineCommon;
import com.ibm.disni.rdma.RdmaActiveEndpoint;
import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.RdmaEndpoint;
import com.ibm.disni.rdma.RdmaEndpointFactory;
import com.ibm.disni.rdma.verbs.*;
import com.sun.corba.se.impl.encoding.CodeSetConversion;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * java -cp disni-1.7-jar-with-dependencies.jar:disni-1.7-tests.jar com.ibm.disni.examples.MySendRecvClient -a 10.10.0.97 -p 11919
 */
public class MySendRecvClient implements RdmaEndpointFactory<MySendRecvClient.ClientEndpoint> {
    private RdmaActiveEndpointGroup<ClientEndpoint> endpointGroup;
    private String host;
    private int port;

    @Override
    public ClientEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new ClientEndpoint(endpointGroup, id, serverSide);
    }

    public void run() throws Exception{
        endpointGroup = new RdmaActiveEndpointGroup<>(1000, false, 128, 4, 128);
        endpointGroup.init(this);

        //创建一个endpoint
        ClientEndpoint endpoint = endpointGroup.createEndpoint();

        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress socketAddr = new InetSocketAddress(ipAddress, port);
        endpoint.connect(socketAddr, 1000);
        System.out.println("Client::server connected");

        //向server发送一个消息
        ByteBuffer sendBuffer = endpoint.getSendBuffer();
        sendBuffer.asCharBuffer().put("Hello, Server. This is Client.");
        sendBuffer.clear();

        endpoint.postSend(endpoint.getWrList_send()).execute().free();
        IbvWC wc = endpoint.getWcList().take();
        System.out.println("Client::the id of the message is " + wc.getWr_id());

        //接收来自server的消息
        endpoint.getWcList().take();
        ByteBuffer recvBuffer = endpoint.getRecvBuffer();
        recvBuffer.clear();
        System.out.println("Client:: message from the server is " + recvBuffer.asCharBuffer().toString());

        //close all things
        endpoint.close();
        endpointGroup.close();
        System.out.println("Client::close client");
    }

    public void launch(String[] args) {
        CmdLineCommon cmd = new CmdLineCommon("SendRecvServer");

        try {
            cmd.parse(args);
        } catch (ParseException e){
            cmd.printHelp();
            System.exit(-1);
        }

        this.host = cmd.getIp();
        this.port = cmd.getPort();

        try {
            this.run();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static void main(String[] args) {
        MySendRecvClient client = new MySendRecvClient();
        client.launch(args);
    }

    public static class ClientEndpoint extends RdmaActiveEndpoint {
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

        public ClientEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
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

            wcList = new ArrayBlockingQueue<>(10);
        }

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
            sendWR.setWr_id(1314);
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
            recvWR.setWr_id(1315);
            wrList_recv.add(recvWR);

            System.out.println("Client::initiated receive");
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

        public LinkedList<IbvRecvWR> getWrList_recv() {
            return wrList_recv;
        }

        public ArrayBlockingQueue<IbvWC> getWcList() {
            return wcList;
        }
    }

}
