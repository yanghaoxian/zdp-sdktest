package com.example.demosdk;

import com.alibaba.fastjson.JSONObject;
import com.inspur.zdp.sdk.*;
import com.inspur.zdp.service.pb.grpc.*;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * @author: yanghaoxian
 * @created: 2023/03/22
 * @description:
 */

@Service

public class Runner implements ApplicationRunner, ApplicationContextAware {

    @Value("${zdp.ip}")
    private String ip;
    @Value("${zdp.port}")
    private String port;
    @Value("${zdp.file}")
    private String file;
    @Value("${zdp.use.file}")
    private Boolean useFile;
    @Value("${zdp.groupName}")
    private String groupName;
    @Value("${zdp.endpointName}")
    private String endpointName;
    @Value("${zdp.deviceName}")
    private String deviceName;

    @Value("${zdp.topicName}")
    private String topicName;

    ApplicationContext applicationContext;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Logger log = Logger.getLogger(Runner.class.getName());
        ZDPConnectionInfo zdpConnectionInfo = new ZDPConnectionInfo();
        zdpConnectionInfo.setIp(ip);
        zdpConnectionInfo.setPort(port);
        if (useFile) {
            zdpConnectionInfo.setX509FilePath(file);
        }
        ZDPConnection zdpConnection = ZDPConnection.connect(zdpConnectionInfo);
        ZDPPortal zdpPortal = new ZDPPortal(zdpConnection);
        zdpPortal.createDeviceGroup(groupName);
        log.info("create device group:"+groupName);
        ZDPDevice zdpDevice = zdpPortal.createDevice(deviceName,groupName);
        log.info("create device:"+deviceName);
        ZDPEndpoint endpoint = zdpDevice.createEndpoint(endpointName, ZDPDataType.DOUBLE, ZDPEndpoint.CaptureProtocol.MODUBUS, 1000, 1000*60);
        log.info("create endpoint+"+endpointName);
        StringBuilder stringBuilder = new StringBuilder();
        int data = new Random().nextInt();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        stringBuilder.append("{\n")
                .append("\t\"values\": [{\n")
                .append("\t\t\"timestamp\": \"").append(sdf.format(new Date())).append("\",\n")
                .append("\t\t\"value\": ").append(data).append(",\n")
                .append("\t\t\"valid\": 1\n")
                .append("\t}]\n")
                .append("}");
        log.info("save endpoint");
        log.info(stringBuilder.toString());
        // 调用zdp保存数据
        endpoint.saveDataToEndpoint(stringBuilder.toString());
        DevEndpointsName.Builder builder = DevEndpointsName.newBuilder().setDeviceName(deviceName);
        builder.addEndpointsName(endpointName);
        LatestEndpointsRequest latestEndpointsRequest = LatestEndpointsRequest.newBuilder()
                .addDevsEndpointsName(builder.build())
                .build();
        LatestEndpointsResponse response = zdpPortal.queryEndpointsLatest(latestEndpointsRequest);
        log.info("latest endpoint:"+response.toString());
        EndpointsRequest request = EndpointsRequest.newBuilder()
                .addDevicesEndpointsTs(
                        DeviceEndpointTs.newBuilder()
                                .setDeviceName(deviceName)
                                .addEndpointsTs(
                                        EndpointTs.newBuilder()
                                                .setEndpointName(endpointName)
                                                .addStartsEndsTs(
                                                        TS.newBuilder()
                                                                .setStartTs(1642667244L)
                                                                .setEndTs(System.currentTimeMillis()/1000)
                                                                .build())
                                                .build())
                                .build())
                .build();
        EndpointsByTSRangeResponse endpointsByTSRangeResponse = zdpPortal.queryEndpoints(request);
        log.info("queryAllPoint");
        for(DevEndpointValue device : endpointsByTSRangeResponse.getDevEndpointValuesList()){
            for (EndpointData endpointData : device.getEndpointDatasList()){
                for(DoublePoint doublePoint : endpointData.getTsValues().getDoubleValuesList()){
                    log.info("ts:"+doublePoint.getTimestamp()+",value"+doublePoint.getValue());
                }
            }
        }
        log.info("test create topic:"+topicName);
        zdpPortal.getZDPPubSub().createPublishTopic(topicName);
        log.info("test subscribe topic");
        zdpPortal.getZDPPubSub().registerDataListener(PubsubDataType.CUSTOM_STRING, new ZDPDataListener<String>(topicName){
            @Override
            public void onData(String s) {
                log.info("consume message:"+s);
            }
        });

        Long start = System.currentTimeMillis();
        int i=0;
        log.info("test publish topic");
        while(i<10){
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("time",System.currentTimeMillis());
            String uuid = UUID.randomUUID().toString();
            jsonObject.put("value", uuid);
            log.info("produce message:"+jsonObject.toString());
            zdpPortal.getZDPPubSub().publish(topicName, jsonObject.toString());

            i++;
            Thread.sleep(1000);
        }
        ConfigurableApplicationContext cyx = (ConfigurableApplicationContext) applicationContext;
        cyx.close();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
