/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package adaboost;

import java.util.List;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.types.BytesValue;
import cgl.imr.types.StringValue;
import cgl.imr.types.IntKey;
import cgl.imr.types.DoubleVectorData;

public class AdaBoostReducer implements ReduceTask {
  
    //DoubleVectorData results;
    
    public void close() throws TwisterException {
        // TODO Auto-generated method stub
    }
    
    public void configure(JobConf jobConf, ReducerConf reducerConf)
    throws TwisterException {
    }
    
    public void reduce(ReduceOutputCollector collector, Key key, List<Value> values) throws TwisterException {
        
        if (values.size() <= 0) {
            throw new TwisterException("Reduce input error no values.");
        }
        
        try {
            double pos_cor=0.0;
            double neg_cor=0.0;
            int iClass=0;
            double vert_id=0.0;
            double mistake_prob=0.0;
            int numValues = values.size();
            double[] est_vals = new double[2*numValues];
            for(int i=0;i<numValues;i++) {
                StringValue val = (StringValue) values.get(i);
                String[] val_vector = val.toString().split(" ");
                iClass = Integer.parseInt(val_vector[0]);
                mistake_prob = Double.parseDouble(val_vector[1]);
                
                //below lines added to add q_vals to final output
                vert_id = Double.parseDouble(val_vector[2]);
                est_vals[2*i]=vert_id;
                est_vals[2*i+1]=mistake_prob;
                //
                
                if(iClass==1) pos_cor+=mistake_prob;
                else if(iClass==-1) neg_cor+=mistake_prob;
                if(neg_cor<.0000000001) neg_cor=0.0000000001;
                if(pos_cor<.0000000001) pos_cor=0.0000000001;
            }
            double eta=0.5;
            double weight = eta*(double)Math.log10(pos_cor/neg_cor);
            IntKey key_int = new IntKey(key.getBytes());
            int key_val = key_int.getKey();
            //System.out.println(key_val + ": " + weight);
            
            
            double[][] weight_vec = new double[1][3+2*numValues];
            //double[] w_vec = {weight, pos_cor, neg_cor};
            weight_vec[0][0]=weight;weight_vec[0][1]=pos_cor;weight_vec[0][2]=neg_cor;
            for(int i=0;i<2*numValues;i++) weight_vec[0][3+i]=est_vals[i];
            DoubleVectorData weightData = new DoubleVectorData(weight_vec, 1, 3+2*numValues);
            
            
            collector.collect(key, new BytesValue(weightData.getBytes()) );
        } catch (SerializationException e) {
            throw new TwisterException(e);
        }
    }
}
