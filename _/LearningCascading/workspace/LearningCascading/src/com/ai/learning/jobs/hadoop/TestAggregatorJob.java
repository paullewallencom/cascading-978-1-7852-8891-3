/* Copyright (c) 2015, Analytics Inside LLC. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *   - Neither the name of Analytics Inside LLC or the names of its
 *     products or trademarks,  may be used to endorse or promote products 
 *     derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */ 

package com.ai.learning.jobs.hadoop;

import java.util.Properties;

import com.ai.learning.SampleAggregator2;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.hadoop.Hfs;


public class TestAggregatorJob {
	// We have a list that consists of store name, product, how many sold, and how many returned
	// Task to calculate how many products total are kept (i.e sold and not returned) 
	// Accepts two args - input file and output file
	// 
	public static void main(String[] args) {
		// define source and sink Taps.
		Scheme sourceScheme = new TextDelimited( new Fields("store_name", "product_name", "number_sold", "number_returned"), true, "," );
		Tap source = new Hfs( sourceScheme, args[0] );
		Scheme sinkScheme = new TextDelimited( new Fields( "product_name", "total_kept") );
		Tap sink = new Hfs( sinkScheme, args[1], SinkMode.REPLACE );
		
		// the 'head' of the pipe assembly
		Pipe assembly = new Pipe( "total" );
		assembly = new GroupBy(assembly, new Fields("product_name")); 
		assembly = new Every(assembly, new Fields("number_sold","number_returned"),  new SampleAggregator2(new Fields("total_kept")), Fields.ALL);
		
		Properties properties = new Properties();
		AppProps.setApplicationJarClass( properties, com.ai.learning.jobs.hadoop.TestAggregatorJob.class );
		FlowConnector flowConnector = new HadoopFlowConnector(properties);
		Flow flow = flowConnector.connect(source,sink,assembly);
		flow.complete();
	}

}
