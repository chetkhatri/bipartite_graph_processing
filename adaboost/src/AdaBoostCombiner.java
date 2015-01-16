/*
 * Software License, Version 1.0
 *
 *  Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 *
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1) All redistributions of source code must retain the above copyright notice,
 *  the list of authors in the original source code, this list of conditions and
 *  the disclaimer listed in this license;
 * 2) All redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the disclaimer listed in this license in
 *  the documentation and/or other materials provided with the distribution;
 * 3) Any documentation included with all redistributions must include the
 *  following acknowledgement:
 *
 * "This product includes software developed by the Community Grids Lab. For
 *  further information contact the Community Grids Lab at
 *  http://communitygrids.iu.edu/."
 *
 *  Alternatively, this acknowledgement may appear in the software itself, and
 *  wherever such third-party acknowledgments normally appear.
 *
 * 4) The name Indiana University or Community Grids Lab or Twister,
 *  shall not be used to endorse or promote products derived from this software
 *  without prior written permission from Indiana University.  For written
 *  permission, please contact the Advanced Research and Technology Institute
 *  ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 * 5) Products derived from this software may not be called Twister,
 *  nor may Indiana University or Community Grids Lab or Twister appear
 *  in their name, without prior written permission of ARTI.
 *
 *
 *  Indiana University provides no reassurances that the source code provided
 *  does not infringe the patent or any other intellectual property rights of
 *  any other entity.  Indiana University disclaims any liability to any
 *  recipient for claims brought by any other entity based on infringement of
 *  intellectual property rights or otherwise.
 *
 * LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO
 * WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 * NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF
 * INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS.
 * INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS",
 * "VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.
 * LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR
 * ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION
 * GENERATED USING SOFTWARE.
 */

package adaboost;

import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import cgl.imr.base.Combiner;
import cgl.imr.base.Key;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.types.BytesValue;
import cgl.imr.types.StringValue;
import cgl.imr.types.IntKey;
import cgl.imr.types.DoubleVectorData;

/**
 * Convert the set of bytes representing the Value object into a ByteValue.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */
public class AdaBoostCombiner implements Combiner {

	DoubleVectorData results;

	public AdaBoostCombiner() {
		results = new DoubleVectorData();
	}

	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}

	/***
	 * Combines the reduce outputs to a single value.
	 */
	public void combine(Map<Key, Value> keyValues) throws TwisterException {
		assert (keyValues.size() == 1);// There should be a single value here.
        System.out.println("in combine");
        try {
            int num_keys = keyValues.keySet().size();
            
            //double[][] output_weights = new double[num_keys][4];
            
            //System.out.println("num keys: " + num_keys);
            //results = new DoubleVectorData();
            Iterator<Key> ite = keyValues.keySet().iterator();
            int max_length=0;
            List<double[]> output_list = new ArrayList<double[]>();
            while (ite.hasNext()) {
                Key key = ite.next();
                IntKey int_key = new IntKey(key.getBytes());
                int key_val = int_key.getKey();
                //System.out.println("Int Key: " + key_val);
                //BytesValue val = (BytesValue) keyValues.get(key);
                
                
                /*
                 StringValue strVal = (StringValue) keyValues.get(key);
                */
                BytesValue val = (BytesValue) keyValues.get(key);
                DoubleVectorData weight_vec = new DoubleVectorData();
                weight_vec.fromBytes(val.getBytes());
                
                double[] output_weights = new double[weight_vec.getData().length+1];
                if(output_weights.length > max_length) max_length=output_weights.length;
                /*output_weights[key_val-1][0]=key_val;
                /*
                 output_weights[key_val-1][1]=Double.parseDouble(strVal.toString());
                */
                /*
                output_weights[key_val-1][1]=weight_vec.getData()[0][0];
                output_weights[key_val-1][2]=weight_vec.getData()[0][1];
                output_weights[key_val-1][3]=weight_vec.getData()[0][2];
                */
                
                output_weights[0]=key_val;
                for(int i=1;i<output_weights.length;i++) output_weights[i]=weight_vec.getData()[i-1];
                output_list.add(output_weights);
                
            //System.out.println("strval: " + strVal.toString());
            }
            
            double[][] output_double_arr = new double[num_keys][max_length];
            double[] max_len_row = new double[max_length];
            for(int i=0;i<num_keys;i++) {
                int row_len = output_list.get(i).length;
                int row_len_diff_from_max = max_length - row_len;
                double[] row = new double[row_len];
                row = output_list.get(i);
                for (int j=0;j<row_len;j++) max_len_row[j]=row[j];
                for (int k=0;k<row_len_diff_from_max;k++) max_len_row[k+row_len] = 0.0;
                output_double_arr[i]=max_len_row;
            }
            
            this.results = new DoubleVectorData(output_weights, num_keys, max_length);
            //this.results.fromBytes(strVal.getBytes());
		} catch (SerializationException e) {
			throw new TwisterException(e);
		}
	}

	public void configure(JobConf jobConf) throws TwisterException {
		// TODO Auto-generated method stub

	}

	public DoubleVectorData getResults() {
		return results;
	}
}
