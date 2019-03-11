package com.ibm.disni.examples;

import com.ibm.disni.CmdLineCommon;
import com.ibm.disni.rdma.RdmaActiveEndpoint;
import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.RdmaEndpointFactory;
import com.ibm.disni.rdma.verbs.*;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * java -cp disni-1.7-jar-with-dependencies.jar:disni-1.7-tests.jar com.ibm.disni.examples.WriteClient -a 10.10.0.97 -p 11919
 */
public class WriteClient implements RdmaEndpointFactory<WriteClient.ClientEndpoint> {
    private RdmaActiveEndpointGroup<ClientEndpoint> endpointGroup;
    private String host;
    private int port;

    @Override
    public ClientEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new ClientEndpoint(endpointGroup, id, serverSide);
    }


    public void run() throws Exception{
        //创建一个endpointGroup，并用factory初始化
        endpointGroup = new RdmaActiveEndpointGroup<>(1000, false, 128, 25, 128);
        endpointGroup.init(this);

        //创建一个endpoint
        ClientEndpoint endpoint = endpointGroup.createEndpoint();
        System.out.println("Client::endpoint created.");

        //连接server
        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);
        endpoint.connect(address, 1000);
        InetSocketAddress socketAddr = (InetSocketAddress) endpoint.getDstAddr();
        System.out.println("Client::server connected, address is " + socketAddr.toString());

        //接收数据
        endpoint.getWcList().take();
        ByteBuffer recvBuffer = endpoint.getRecvBuffer();
        long rAddr = recvBuffer.getLong();
        int length = recvBuffer.getInt();
        int rkey = recvBuffer.getInt();
        recvBuffer.clear();
        System.out.println("Client::address is " + rAddr + ", length is " + length + ", rkey is " + rkey);

        ByteBuffer dataBuffer = endpoint.getDataBuffer();
        dataBuffer.asCharBuffer().put("Server, client write to your buffer.");

        IbvSendWR sendWR = endpoint.getSendWR();
        sendWR.setWr_id(2000);
        sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_WRITE);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR.getRdma().setRemote_addr(rAddr);
        sendWR.getRdma().setRkey(rkey);

        SVCPostSend postSend = endpoint.postSend(endpoint.getWrList_send());
        postSend.getWrMod(0).getSgeMod(0).setLength(length);
        postSend.execute();

        endpoint.getWcList().take();
        System.out.println("Client::message send");

        sendWR.setWr_id(2001);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        endpoint.postSend(endpoint.getWrList_send()).execute().free();
        endpoint.getWcList().take();
        System.out.println("Client::final message send");

        //close everything
        endpoint.close();
        endpointGroup.close();
    }

    public void launch(String[] args) throws Exception {
        CmdLineCommon cmd = new CmdLineCommon("ReadClient");

        try {
            cmd.parse(args);
        } catch (ParseException e) {
            cmd.printHelp();
            System.exit(-1);
        }

        this.host = cmd.getIp();
        port = cmd.getPort();

        this.run();
    }


    public static void main(String[] args) throws Exception {
        WriteClient client = new WriteClient();
        client.launch(args);
    }

    public static class ClientEndpoint extends RdmaActiveEndpoint {
        private ByteBuffer[] buffers;
        private IbvMr[] mrList;
        private int bufferCount;
        private int bufferSize;

        private ByteBuffer dataBuffer;
        private IbvMr dataMr;
        private ByteBuffer recvBuffer;
        private IbvMr recvMr;
        private ByteBuffer sendBuffer;
        private IbvMr sendMr;

        private IbvRecvWR recvWR;
        private LinkedList<IbvRecvWR> wrList_recv;
        private IbvSge sgeRecv;
        private LinkedList<IbvSge> sgeList_recv;

        private IbvSge sgeSend;
        private LinkedList<IbvSge> sgeList_send;
        private IbvSendWR sendWR;
        private LinkedList<IbvSendWR> wrList_send;

        private ArrayBlockingQueue<IbvWC> wcList;

        public ClientEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
            super(group, idPriv, serverSide);

            bufferCount = 3;
            bufferSize = 100;
            buffers = new ByteBuffer[bufferCount];
            mrList = new IbvMr[bufferCount];

            for (int i = 0; i < bufferCount; i++) {
                buffers[i] = ByteBuffer.allocateDirect(bufferSize);
            }

            sgeRecv = new IbvSge();
            sgeList_recv = new LinkedList<>();
            recvWR = new IbvRecvWR();
            wrList_recv = new LinkedList<>();

            sgeSend = new IbvSge();
            sgeList_send = new LinkedList<>();
            sendWR = new IbvSendWR();
            wrList_send = new LinkedList<>();

            wcList = new ArrayBlockingQueue<>(10);
        }

        @Override
        protected synchronized void init() throws IOException {
            super.init();

            //注册memory region
            for (int i = 0; i < bufferCount; i++) {
                mrList[i] = registerMemory(buffers[i]).execute().free().getMr();
            }

            //初始化数据缓冲区和接收缓冲区
            dataBuffer = buffers[0];
            dataMr = mrList[0];
            recvBuffer = buffers[2];
            recvMr = mrList[2];
            sendBuffer = buffers[1];
            sendMr = mrList[1];

            dataBuffer.clear();
            recvBuffer.clear();

            sgeSend.setAddr(dataMr.getAddr());
            sgeSend.setLength(dataMr.getLength());
            sgeSend.setLkey(dataMr.getLkey());
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
            recvWR.setSg_list(sgeList_recv);
            recvWR.setWr_id(2001);
            wrList_recv.add(recvWR);

            System.out.println("WriteClient::initial receive");
            this.postRecv(wrList_recv).execute().free();
        }

        @Override
        public void dispatchCqEvent(IbvWC wc) {
            wcList.add(wc);
        }

        public LinkedList<IbvSendWR> getWrList_send() {
            return wrList_send;
        }

        public ArrayBlockingQueue<IbvWC> getWcList() {
            return wcList;
        }

        public ByteBuffer getRecvBuffer() {
            return recvBuffer;
        }

        public IbvSendWR getSendWR() {
            return sendWR;
        }

        public ByteBuffer getDataBuffer() {
            return dataBuffer;
        }
    }
}
