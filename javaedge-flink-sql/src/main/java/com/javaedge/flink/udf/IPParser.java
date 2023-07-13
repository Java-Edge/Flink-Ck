package com.javaedge.flink.udf;

import com.javaedge.flink.utils.IPUtil;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class IPParser extends ScalarFunction {


    IPUtil ipUtil = null;

    @Override
    public void open(FunctionContext context) throws Exception {
        ipUtil = IPUtil.getInstance();
    }

    @Override
    public void close() throws Exception {
        if(null != ipUtil) {
            ipUtil = null;
        }
    }

    public String eval(String ip) {
        String[] infos = ipUtil.getInfos(ip);

        return infos[1] + "-" + infos[2];
    }
}
