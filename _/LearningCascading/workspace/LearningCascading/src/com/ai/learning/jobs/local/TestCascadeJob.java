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

package com.ai.learning.jobs.local;
import com.ai.learning.SentFunc;
import com.ai.learning.TokenBuffer;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowSkipIfSinkExists;
import cascading.flow.FlowSkipStrategy;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

// Simple Cascade test for two flows: sentence splitter and tokenizer
// args[0] - input file, args[1] - sentence output, args[2] - token output
public class TestCascadeJob {

	public static void main(String[] args) {


		Fields fieldDeclarationInput = new Fields("document","text");
		Fields fieldDeclarationInterim = new Fields( "document","sentnum", "sentence");
		Fields fieldDeclarationOutput = new Fields( "documentname","sentnumber","wordnum", "word");

		Scheme inputScheme = new TextDelimited( fieldDeclarationInput, true,"\t");
		Scheme interimScheme = new TextDelimited( fieldDeclarationInterim, "\t");
		Scheme outputScheme = new TextDelimited( fieldDeclarationOutput, "\t");

		Tap docTap = new FileTap (inputScheme, args[0]);
		Tap sink1Tap = new FileTap(interimScheme, args[1]);
		Tap sink2Tap = new FileTap(outputScheme, args[2], SinkMode.REPLACE );

		Pipe inPipe = new Pipe("InPipe"); 

		inPipe= new Each(inPipe, new SentFunc());	
		inPipe = new Each(inPipe, new Debug());	

		Flow flow1 = new LocalFlowConnector().connect( "Flow1", docTap, sink1Tap, inPipe );


		Pipe inPipe2=new Pipe("InPipe2"); 
		inPipe2 = new GroupBy(inPipe2, new Fields("document"), new Fields("sentnum"));

		inPipe2 = new Every(inPipe2, new TokenBuffer(), fieldDeclarationOutput);	
		inPipe2 = new Each(inPipe2, new Debug());	

		Flow flow2 = new LocalFlowConnector().connect( "Flow2", sink1Tap, sink2Tap, inPipe2);

		CascadeConnector connector = new CascadeConnector();

		Cascade cascade = connector.connect( flow2, flow1 );
		// Set flow skip strategy for the cascade
		cascade.setFlowSkipStrategy(new FlowSkipIfSinkExists());
		cascade.complete();
	}
}

