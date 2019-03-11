package com.ibm.disni.examples;

import com.ibm.disni.CmdLineCommon;
import com.ibm.disni.rdma.*;
import com.ibm.disni.rdma.verbs.*;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;


/**
 * java -cp disni-1.7-jar-with-dependencies.jar:disni-1.7-tests.jar com.ibm.disni.examples.WriteServer -a 10.10.0.97 -p 11919
 */
public class WriteServer implements RdmaEndpointFactory<WriteServer.ServerEndpoint> {
    private RdmaActiveEndpointGroup<ServerEndpoint> endpointGroup;
    private String host;
    private int port;

    @Override
    public ServerEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new ServerEndpoint(endpointGroup, id, serverSide);
    }

    public void run() throws Exception{
        //创建endpoint group,并用factory初始化
        endpointGroup = new RdmaActiveEndpointGroup<>(1000, false, 128, 4, 128);
        endpointGroup.init(this);

        //创建server endpoint
        RdmaServerEndpoint<ServerEndpoint> serverEndpoint = endpointGroup.createServerEndpoint();
        //绑定端口
        InetAddress ipaddr = InetAddress.getByName(host);
        InetSocketAddress socketAddr = new InetSocketAddress(ipaddr, port);

        try {
            serverEndpoint.bind(socketAddr, 10);
        } catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
        }
        System.out.println("Server::server bound to " + socketAddr.toString());

        //接收rdma连接
        ServerEndpoint endpoint = serverEndpoint.accept();
        System.out.println("Server::connection accept");

        //发送send操作
        IbvMr dataMr = endpoint.getDataMr();
        ByteBuffer sendBuffer = endpoint.getSendBuffer();
        sendBuffer.putLong(dataMr.getAddr());
        sendBuffer.putInt(dataMr.getLength());
        sendBuffer.putInt(dataMr.getLkey());
        sendBuffer.clear();

        endpoint.postSend(endpoint.getWrList_send()).execute().free();
        endpoint.getWcList().take();
        System.out.println("Server::send memory region information");

        //接收数据
        endpoint.getWcList().take();
        System.out.println("Server::receive the message from client");

        ByteBuffer dataBuffer = endpoint.getDataBuffer();
        System.out.println("Server::message from client: " + dataBuffer.asCharBuffer().toString());

        //close everything
        endpoint.close();
        endpointGroup.close();
        serverEndpoint.close();
    }

    public void launch(String[] args) throws Exception{
        CmdLineCommon cmd = new CmdLineCommon("ReadServer");

        try {
            cmd.parse(args);
        } catch (ParseException e) {
            cmd.printHelp();
            System.exit(-1);
        }
        this.host = cmd.getIp();
        this.port = cmd.getPort();

        this.run();
    }

    public static void main(String[] args) throws Exception{
        WriteServer server = new WriteServer();
        server.launch(args);
    }



    public static class ServerEndpoint extends RdmaActiveEndpoint {
        private ByteBuffer[] buffers;
        private IbvMr[] mrLists;
        private int bufferCount;
        private int bufferSize;

        private ByteBuffer dataBuffer;
        private IbvMr dataMr;
        private ByteBuffer sendBuffer;
        private IbvMr sendMr;
        private ByteBuffer recvBuffer;
        private IbvMr recvMr;

        private IbvSge sgeSend;
        private LinkedList<IbvSge> sgeList_send;
        private IbvSendWR sendWR;
        private LinkedList<IbvSendWR> wrList_send;

        private IbvSge sgeRecv;
        private LinkedList<IbvSge> sgeList_recv;
        private IbvRecvWR recvWR;
        private LinkedList<IbvRecvWR> wrList_recv;

        private ArrayBlockingQueue<IbvWC> wcList;

        public ServerEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
            super(group, idPriv, serverSide);

            //为当前endpoint申请memory region
            bufferCount = 3;
            bufferSize = 100;
            buffers = new ByteBuffer[bufferCount];
            mrLists = new IbvMr[bufferCount];


            //初始化缓冲区
            for (int i = 0; i < bufferCount; i++) {
                buffers[i] = ByteBuffer.allocateDirect(bufferSize);
            }

            sgeSend = new IbvSge();
            sgeList_send = new LinkedList<>();
            sendWR = new IbvSendWR();
            wrList_send = new LinkedList<>();

            sgeRecv = new IbvSge();
            sgeList_recv = new LinkedList<>();
            recvWR = new IbvRecvWR();
            wrList_recv = new LinkedList<>();

            System.out.println("Server::server endpoint is constructed");
            this.wcList = new ArrayBlockingQueue<>(10);
        }

        @Override
        protected synchronized void init() throws IOException {
            super.init();

            //初始化memory region
            for (int i = 0; i < bufferCount; i++) {
                mrLists[i] = registerMemory(buffers[i]).execute().free().getMr();
            }

            dataBuffer = buffers[0];
            dataMr = mrLists[0];
            sendBuffer = buffers[1];
            sendMr = mrLists[1];
            recvBuffer = buffers[2];
            recvMr = mrLists[2];

            sendBuffer.clear();
            dataBuffer.clear();
            recvBuffer.clear();

            sgeSend.setAddr(sendMr.getAddr());
            sgeSend.setLength(sendMr.getLength());
            sgeSend.setLkey(sendMr.getLkey());
            sgeList_send.add(sgeSend);
            sendWR.setWr_id(2000);
            sendWR.setSg_list(sgeList_send);
            sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
            sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
            wrList_send.add(sendWR);

            sgeRecv.setAddr(recvMr.getAddr());
            sgeRecv.setLength(recvMr.getLength());
            sgeRecv.setLkey(recvMr.getLkey());
            sgeList_recv.add(sgeRecv);
            recvWR.setWr_id(2001);
            recvWR.setSg_list(sgeList_recv);
            wrList_recv.add(recvWR);

            System.out.println("WriteServer::server initial");
            this.postRecv(wrList_recv).execute();
        }

        @Override
        public void dispatchCqEvent(IbvWC wc) throws IOException {
            wcList.add(wc);
        }

        public ArrayBlockingQueue<IbvWC> getWcList() {
            return wcList;
        }

        public LinkedList<IbvSendWR> getWrList_send() {
            return wrList_send;
        }

        public ByteBuffer getSendBuffer() {
            return sendBuffer;
        }

        public IbvMr getDataMr() {
            return dataMr;
        }

        public ByteBuffer getDataBuffer() {
            return dataBuffer;
        }
    }
}
