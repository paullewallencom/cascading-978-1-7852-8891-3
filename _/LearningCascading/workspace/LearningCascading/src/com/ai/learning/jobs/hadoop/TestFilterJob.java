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
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.flow.Flow;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import com.ai.learning.SampleFilter;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.hadoop.Hfs;
import cascading.property.AppProps;
import java.util.Properties;

/*
 * A job to test our SampleFilter. Filter out all of the tuples with word "test" in the text line
 */
public class TestFilterJob {

	public static void main(String[] args) {


		// define source and sink Taps.
		Scheme mainScheme = new TextDelimited( new Fields( "line_num", "line" ), true, "\t" );
		Tap source = new Hfs( mainScheme, args[0] );
		Tap sink = new Hfs( mainScheme, args[1], SinkMode.REPLACE );
		
		Pipe inPipe = new Pipe("InPipe"); 

		inPipe = new Each(inPipe, new SampleFilter("test"));
		
		Properties properties = new Properties();
		AppProps.setApplicationJarClass( properties, com.ai.learning.jobs.hadoop.TestFilterJob.class );
		Flow flow = new HadoopFlowConnector(properties).connect("filterFlow", source, sink, inPipe );
		flow.complete();
	}
}


