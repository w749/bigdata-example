package org.mlamp.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class OrderReducer extends Reducer<Text, OrderBean, Text, OrderBean> {
    @Override
    protected void reduce(Text key, Iterable<OrderBean> values, Reducer<Text, OrderBean, Text, OrderBean>.Context context) throws IOException, InterruptedException {
        ArrayList<OrderBean> orders = new ArrayList();
        OrderBean orderProduct = new OrderBean();
        values.forEach(value -> {
            OrderBean tmpOrderBean = new OrderBean();
            if ("joinProduct".equals(value.getFileName())) {
                try {
                    BeanUtils.copyProperties(orderProduct, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    BeanUtils.copyProperties(tmpOrderBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
                orders.add(tmpOrderBean);
            }
        });
        orders.forEach(order -> {
            order.setOrderName(orderProduct.getOrderName());
            try {
                context.write(key, order);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
