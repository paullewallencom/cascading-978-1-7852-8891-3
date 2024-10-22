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
import cascading.flow.FlowDef;
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
import com.ai.learning.RelationshipBuffer;
import com.ai.learning.SentFunc;
import com.ai.learning.TokenBuffer;

// This job is to test relationships among named entities
// args[0] - initial data file, args[1] - named entity output, args[2] - relationship output

public class TestNLPRelationshipsJob {

	public static void main(String[] args) {
		Fields fieldDeclarationInput = new Fields("document","text");

		Fields fieldDeclarationOutputInterim = new Fields( "documentname","sentnumber","wordnum", "word");
		Fields fieldDeclarationOutput = new Fields( "documentname","sentnumber","word","type");
		Fields fieldDeclarationRelationships = new Fields( "documentname","sentnumber","word1","type1","word2","type2");

		Scheme inputScheme = new TextDelimited( fieldDeclarationInput, true,"\t");
		Scheme outputScheme = new TextDelimited( fieldDeclarationOutput, "\t");
		Scheme relScheme = new TextDelimited(fieldDeclarationRelationships, "\t");

		Tap docTap = new FileTap (inputScheme, args[0]);

		Tap finalOutTap = new FileTap(outputScheme, args[1]);
		Tap relOutTap = new FileTap(relScheme, args[2]);

		Pipe headPipe = new Pipe("HeadPipe"); 
		headPipe= new Each(headPipe, new SentFunc());	
		//headPipe = new Each(headPipe, new Debug());	
		headPipe = new GroupBy(headPipe, new Fields("document"), new Fields("sentnum"));
		headPipe = new Every(headPipe, new TokenBuffer(), fieldDeclarationOutputInterim);	

		// our custom NamedEntityExtractorHDFS SubAssembly
		SubAssembly namedEntityPipe = new NamedEntityExtractor(headPipe);

		// grab the split branches
		Pipe people =  new Pipe("people",namedEntityPipe.getTails()[ 0 ]) ;
		Pipe orgs =  new Pipe ("organizations",namedEntityPipe.getTails()[ 1 ]);
		Pipe chemicals= new Pipe ("chemicals", namedEntityPipe.getTails()[ 2 ]);

		Pipe inPipe= new Merge(people,orgs, chemicals);

		// Relationship pipe is based on inPipe grouped by document name and sentence number
		System.out.println("Starting relationship extraction");
		Pipe relPipe = new Pipe("realtionshps",inPipe);
		relPipe = new GroupBy(relPipe, new Fields("documentname","sentnumber"));
		relPipe = new Every(relPipe,new RelationshipBuffer(), Fields.RESULTS);

		// one source two sinks
		FlowDef flowDef = new FlowDef()
		.setName( "process" )
		.addSource( headPipe, docTap )
		.addTailSink( inPipe, finalOutTap )
		.addTailSink( relPipe, relOutTap );

		Flow flow = new LocalFlowConnector().connect( flowDef );
		// execute the flow, block until complete
		flow.complete();
		System.out.println("Done");

	}

}
