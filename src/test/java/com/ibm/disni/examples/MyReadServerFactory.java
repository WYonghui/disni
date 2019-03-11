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
import java.util.concurrent.LinkedBlockingQueue;

/**
 * java -cp disni-1.7-jar-with-dependencies.jar:disni-1.7-tests.jar com.ibm.disni.examples.MyReadServerFactory -a 10.10.0.97 -p 11919
 * 2. Implement a factory for your custom endpoints
 */
public class MyReadServerFactory implements RdmaEndpointFactory<MyReadServerFactory.CustomServerEndpoint> {
//    private Logger LOG = LoggerFactory.getLogger(MyReadServerFactory.class);
    private RdmaActiveEndpointGroup<CustomServerEndpoint> endpointGroup;
    private String host;
    private int port;

    @Override
    public CustomServerEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new CustomServerEndpoint(endpointGroup, id, serverSide);
    }

    /*
     * 3. At the server, allocate an endpoint group and initialize it with the factory, create a server endpoint, bind it and accept connections
     * */
    public void run() throws Exception {
        //创建endpointGroup，并适用endpoint Factory初始化它
        endpointGroup = new RdmaActiveEndpointGroup<CustomServerEndpoint>(1000, false, 128, 4, 128);
        endpointGroup.init(this);

        //创建一个serverEndpoint，它可以接受连接
        RdmaServerEndpoint<CustomServerEndpoint> serverEndpoint = endpointGroup.createServerEndpoint();

        //将serverEndpoint绑定到端口
        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);
        serverEndpoint.bind(address, 10);
        System.out.println("Server bind to address " + address.toString());

        //接受连接，产生endpoint
        CustomServerEndpoint endpoint = serverEndpoint.accept();
        System.out.println("Connection accept!");

        //准备一个消息，发往client
        ByteBuffer dataBuffer = endpoint.getDataBuffer();
        ByteBuffer sendBuffer = endpoint.getSendBuffer();
        IbvMr dataMr = endpoint.getDataMr();
        dataBuffer.asCharBuffer().put("This is a RDMA/read on stag " + dataMr.getLkey());
//        System.out.println("the address is " + dataMr.getAddr());
//        System.out.println("the length is " + dataMr.getLength());
//        System.out.println("the lkey is " + dataMr.getLkey());
        dataBuffer.clear();
        sendBuffer.putLong(dataMr.getAddr());
        sendBuffer.putInt(dataMr.getLength());
        sendBuffer.putInt(dataMr.getLkey());
        sendBuffer.clear();

        //post the operation to send the message
        System.out.println("Sending message...");
        endpoint.postSend(endpoint.getWrList_send()).execute().free();
        //等待消息传输完成的event
        endpoint.getWcEvents().take();
        System.out.println("Server sends message successfully");

        //收到final message代表client读取完成
        endpoint.getWcEvents().take();
        ByteBuffer buffer = endpoint.getRecvBuffer();
        System.out.println("ReadServer:: " + buffer.asCharBuffer().toString());

        //释放所有对象
        endpoint.close();
        serverEndpoint.close();
        endpointGroup.close();
    }

    public void launch(String[] args) throws Exception{
        CmdLineCommon cmd = new CmdLineCommon("ReadServer");

        try {
            cmd.parse(args);
        } catch (ParseException e) {
            cmd.printHelp();
            System.exit(-1);
        }
        host = cmd.getIp();
        port = cmd.getPort();

        run();
    }

    public static void main(String[] args) throws Exception{
        MyReadServerFactory readServer = new MyReadServerFactory();
        readServer.launch(args);
    }


    /**
     * 1. define your own custom endpoints by extending either extending RdmaClientEndpoint or RdmaActiveClientEndpoint
     */
    public static class CustomServerEndpoint extends RdmaActiveEndpoint {
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

        private LinkedBlockingQueue<IbvWC> wcEvents;

        public CustomServerEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
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

            wcEvents = new LinkedBlockingQueue<>();
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

            postRecv(wrList_recv).execute();

        }

        @Override
        public void dispatchCqEvent(IbvWC wc) throws IOException {
            wcEvents.add(wc);
        }

        public LinkedBlockingQueue<IbvWC> getWcEvents() {
            return wcEvents;
        }

        public LinkedList<IbvSendWR> getWrList_send() {
            return wrList_send;
        }

        public ByteBuffer getDataBuffer() {
            return dataBuffer;
        }

        public IbvMr getDataMr() {
            return dataMr;
        }

        public ByteBuffer getSendBuffer() {
            return sendBuffer;
        }

        public IbvMr getSendMr() {
            return sendMr;
        }

        public ByteBuffer getRecvBuffer() {
            return recvBuffer;
        }
    }

}
