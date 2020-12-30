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

import com.ai.learning.SentFunc;
import com.ai.learning.TokenBuffer;
import cascading.flow.Flow;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.hadoop.Hfs;
import cascading.property.AppProps;
import java.util.Properties;
/*
 * Test Sentence and Tokenizer processes
 * arg[0] - input file, arg[1] - tokenized output
 */
public class TestNLPSentAndTokenJob {

	public static void main(String[] args) {

		Fields fieldDeclarationInput = new Fields("document","text");
		Fields fieldDeclarationOutput = new Fields( "documentname","sentnumber","wordnum", "word");

		Scheme inputScheme = new TextDelimited( fieldDeclarationInput, true,"\t");
		Scheme outputScheme = new TextDelimited( fieldDeclarationOutput, "\t");
		Tap docTap = new Hfs (inputScheme, args[0]);	
		Tap sinkTap = new Hfs(outputScheme, args[1], SinkMode.REPLACE );
		
		Pipe inPipe = new Pipe("InPipe"); 
		inPipe= new Each(inPipe, new SentFunc());	
		inPipe = new Each(inPipe, new Debug());	
		inPipe = new GroupBy(inPipe, new Fields("document"), new Fields("sentnum"));

		inPipe = new Every(inPipe, new TokenBuffer(), fieldDeclarationOutput);	
		inPipe = new Each(inPipe, new Debug());	

		//initialize app properties, tell Hadoop which jar file to use
		Properties properties = new Properties();
		AppProps.setApplicationJarClass( properties, com.ai.learning.jobs.hadoop.TestNLPSentAndTokenJob.class );

		Flow flow = new HadoopFlowConnector(properties).connect( "process", docTap, sinkTap, inPipe );
		// execute the flow, block until complete
		flow.complete();
	}
}
