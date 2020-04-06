package com.cht.mapreduce.flow.statistics;

import com.cht.mapreduce.statistics.model.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text,Text> {

    private static Logger logger= Logger.getLogger(FlowReducer.class);

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long totalUp=0;
        long totalDown=0;
        for(FlowBean bean : values){
            totalUp+=bean.getUpFlow();
            totalDown+=bean.getDownFlow();
        }

        String result=new FlowBean(totalUp,totalDown).toString();

        logger.info(result);

        context.write(key,new Text(result));
    }
}
