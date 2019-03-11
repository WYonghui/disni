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
 * java -cp disni-1.7-jar-with-dependencies.jar:disni-1.7-tests.jar com.ibm.disni.examples.MyReadClientFactory -a 10.10.0.97 -p 11919
 */
public class MyReadClientFactory implements RdmaEndpointFactory<MyReadClientFactory.CustomClientEndpoint> {
    //    private Logger LOG = LoggerFactory.getLogger(MyReadServerFactory.class);
    private RdmaActiveEndpointGroup<CustomClientEndpoint> endpointGroup;
    private String host;
    private int port;

    @Override
    public CustomClientEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new CustomClientEndpoint(endpointGroup, id, serverSide);
    }

    public void run() throws Exception {
        //创建一个endpointGroup，并用factory初始化
        endpointGroup = new RdmaActiveEndpointGroup<>(1000, false, 128, 25, 128);
        endpointGroup.init(this);

        //创建一个endpoint
        CustomClientEndpoint endpoint = endpointGroup.createEndpoint();
        System.out.println("endpoint created.");

        //连接server
        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);
        endpoint.connect(address, 1000);
        InetSocketAddress addr = (InetSocketAddress) endpoint.getDstAddr();
        System.out.println("connected server, address is " + addr.toString());

        //接收来自server的消息
        endpoint.getWcEvents().take();
        ByteBuffer recvBuffer = endpoint.getRecvBuffer();
        recvBuffer.clear();

        long recvAddr = recvBuffer.getLong();
        int recvLongth = recvBuffer.getInt();
        int recvLkey = recvBuffer.getInt();
        recvBuffer.clear();
        System.out.println("ReadClient::receiving rdma information, addr " + recvAddr + ", length " + recvLongth + ", key " + recvLkey);
        System.out.println("ReadClient::preparing read operation...");

        //上述信息给出了server段消息的保存位置，接下来使用单边操作读取消息
        IbvSendWR sendWR = endpoint.getSendWR();
        sendWR.setWr_id(1001);
        sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR.getRdma().setRemote_addr(recvAddr);
        sendWR.getRdma().setRkey(recvLkey);


        //执行读操作
        SVCPostSend postSend = endpoint.postSend(endpoint.getWrList_send());
        for (int i = 10; i < 100; i += 10) {
            postSend.getWrMod(0).getSgeMod(0).setLength(i);  //a stateful verb call
            postSend.execute();
            endpoint.getWcEvents().take();

            //取出接收到的数据，并打印出来
            ByteBuffer dataBuffer = endpoint.getDataBuffer();
//            System.out.println("the length of dataBuffer is " + dataBuffer.asCharBuffer().length());
//            System.out.println("" + dataBuffer.asCharBuffer().toString());
            dataBuffer.clear();
            System.out.println("ReadClient::read memory from server: " + dataBuffer.asCharBuffer().toString());
        }

        ByteBuffer dataBuffer = endpoint.getDataBuffer();
        dataBuffer.clear();
        dataBuffer.asCharBuffer().put("client has received your message.");

        sendWR.setWr_id(1002);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
//        sendWR.getRdma().setRemote_addr(recvAddr);
//        sendWR.getRdma().setRkey(recvLkey);

        endpoint.postSend(endpoint.getWrList_send()).execute().free();

        //释放所有对象
//        LOG.info("Closing client...");
        System.out.println("Closing client...");
        endpoint.close();
        System.out.println("Closing endpointGroup...");
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

        host = cmd.getIp();
        port = cmd.getPort();
        run();
    }


    public static void main(String[] args) throws Exception {
        MyReadClientFactory readClient = new MyReadClientFactory();
        readClient.launch(args);
    }

    //构造endpoint
    public static class CustomClientEndpoint extends RdmaActiveEndpoint {
        private ByteBuffer[] buffers;
        private IbvMr[] mrList;
        private int bufferCount;
        private int bufferSize;

        private ByteBuffer dataBuffer;
        private IbvMr dataMr;
        private ByteBuffer recvBuffer;
        private IbvMr recvMr;

        private IbvRecvWR recvWR;
        private LinkedList<IbvRecvWR> wrList_recv;
        private IbvSge sgeRecv;
        private LinkedList<IbvSge> sgeList_recv;

        private IbvSge sgeSend;
        private LinkedList<IbvSge> sgeList_send;
        private IbvSendWR sendWR;
        private LinkedList<IbvSendWR> wrList_send;

        private ArrayBlockingQueue<IbvWC> wcEvents;


        public CustomClientEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
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

            wcEvents = new ArrayBlockingQueue<IbvWC>(10);

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

            System.out.println("ReadClient::initial receive");
            this.postRecv(wrList_recv).execute().free();
        }

        @Override
        public void dispatchCqEvent(IbvWC wc) throws IOException {
            wcEvents.add(wc);
        }

        public ArrayBlockingQueue<IbvWC> getWcEvents() {
            return wcEvents;
        }

        public ByteBuffer getRecvBuffer() {
            return recvBuffer;
        }

        public LinkedList<IbvSendWR> getWrList_send() {
            return wrList_send;
        }

        public ByteBuffer getDataBuffer() {
            return dataBuffer;
        }

        public IbvSendWR getSendWR() {
            return sendWR;
        }
    }


}
