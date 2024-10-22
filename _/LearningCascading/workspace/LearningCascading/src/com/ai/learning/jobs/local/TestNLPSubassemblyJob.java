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

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

import com.ai.learning.NamedEntityExtractor;
import com.ai.learning.NamedEntityExtractorHDFS;
import com.ai.learning.SentFunc;
import com.ai.learning.TokenBuffer;
/*
 * Test NLP subassembly for different NLP models and a dictionary
 * arg[0] - input file with text, atg[1] - putput file with named entities and their types
 */
public class TestNLPSubassemblyJob {

	public static void main(String[] args) {
		Fields fieldDeclarationInput = new Fields("document","text");

		Fields fieldDeclarationOutputInterim = new Fields( "documentname","sentnumber","wordnum", "word");
		Fields fieldDeclarationOutput = new Fields( "documentname","sentnumber","word","type");

		Scheme inputScheme = new TextDelimited( fieldDeclarationInput, true,"\t");
		Scheme outputScheme = new TextDelimited( fieldDeclarationOutput, "\t");
		Tap docTap = new FileTap (inputScheme, args[0]);
		Tap finalOutTap = new FileTap(outputScheme, args[1]);
		Pipe headPipe = new Pipe("HeadPipe"); 
		headPipe= new Each(headPipe, new SentFunc());	
		//headPipe = new Each(headPipe, new Debug());	
		headPipe = new GroupBy(headPipe, new Fields("document"), new Fields("sentnum"));
		headPipe = new Every(headPipe, new TokenBuffer(), fieldDeclarationOutputInterim);	
		// our custom SubAssembly
		SubAssembly namedEntityPipe = new NamedEntityExtractor(headPipe);

		// grab the split branches
		Pipe people =  new Pipe("people",namedEntityPipe.getTails()[ 0 ]) ;
		Pipe orgs =  new Pipe ("organizations",namedEntityPipe.getTails()[ 1 ]);
		Pipe chemicals= new Pipe ("chemicals", namedEntityPipe.getTails()[ 2 ]);

		Pipe inPipe= new Merge(people,orgs, chemicals);
		Flow flow = new LocalFlowConnector().connect( "process", docTap, finalOutTap, inPipe );
		// execute the flow, block until complete
		flow.complete();
		System.out.println("Done");
	}
}
