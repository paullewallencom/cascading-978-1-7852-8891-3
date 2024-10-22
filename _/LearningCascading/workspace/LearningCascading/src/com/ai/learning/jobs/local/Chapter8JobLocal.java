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

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowSkipIfSinkExists;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Debug;
import cascading.pipe.Checkpoint;
import cascading.pipe.Each;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

import com.ai.learning.BasicNLPSubAssembly;
import com.ai.learning.NamedEntityExtractor;
import com.ai.learning.RelationshipExtractor;


public class Chapter8JobLocal {
	// args[0] - initial data file, args[1] - named entity output, args[2] - relationship output,
	// args[3] -count output
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Fields fieldDeclarationInput = new Fields("document", "text");

		Fields fieldDeclarationOutputInterim = new Fields("documentname",
				"sentnumber", "wordnum", "word");
		Fields fieldDeclarationOutput = new Fields("documentname",
				"sentnumber", "word", "type");
		Fields fieldDeclarationRelationships = new Fields("documentname",
				"sentnumber", "word1", "type1", "word2", "type2");
		Fields fieldDeclarationCountOutput = new Fields("word", "type", "count");

		Scheme inputScheme = new TextDelimited(fieldDeclarationInput, true,
				"\t");
		Scheme outputScheme = new TextDelimited(fieldDeclarationOutput, true,
				"\t");
		Scheme relScheme = new TextDelimited(fieldDeclarationRelationships,
				true, "\t");
		Scheme countScheme = new TextDelimited(fieldDeclarationCountOutput,
				true, "\t");

		Tap docTap = new FileTap(inputScheme, args[0]);
		Tap finalOutTap = new FileTap(outputScheme, args[1], SinkMode.REPLACE);
		Tap relOutTap = new FileTap(relScheme, args[2], SinkMode.REPLACE);
		Tap countOutTap = new FileTap(countScheme, args[3], SinkMode.REPLACE);

		// Basic sub assembly to extract sentences and tokenize text
		Pipe headPipe = new Pipe("HeadPipe");
		SubAssembly headAssembly = new BasicNLPSubAssembly(headPipe);
		// headPipe = new Each(headPipe, new Debug());

		// Checkpoint code
		//Checkpoint checkpoint = new Checkpoint("checkpoint", headAssembly.getTails()[0]);

		// our custom SubAssembly
		SubAssembly namedEntityPipe = new NamedEntityExtractor(headAssembly.getTails()[0]);

		// grab the split branches
		Pipe people = new Pipe("people", namedEntityPipe.getTails()[0]);

		Pipe orgs = new Pipe("organizations", namedEntityPipe.getTails()[1]);

		Pipe chemicals = new Pipe("chemicals", namedEntityPipe.getTails()[2]);

		Pipe inPipe = new Pipe("NamePipe");
		inPipe = new Merge(people, orgs, chemicals);

		// Flow to extract named entities

		// Tap checkpointTap = new Hfs(new TextDelimited( true, "\t" ), "checkpoint");
		// FlowDef flowBasic = new FlowDef.setName("flowNE")
		//	.addSource(inPipe, docTap)
		//	.addTailSink(inPipe, finalOutTap)
		// 	.addCheckpoint(checkpoint, checkpointTap);
		// Flow flowNamedEntities = new HadoopFlowConnector().connect(flowBasic);

		Flow flowNamedEntities = new LocalFlowConnector().connect("flowNE", docTap,
				finalOutTap, inPipe);
		System.out.println("Finished NE extraction");

		// Flow to extract relationships
		// Relationship pipe on the output by document name and
		// sentence number
		Pipe relPipe = new Pipe("RelationshipPipe");
		SubAssembly relAssembly = new RelationshipExtractor(relPipe);

		FlowDef flowREL = new FlowDef().setName("flowREL")
				.addSource(relPipe, finalOutTap)
				.addTailSink(relAssembly.getTails()[0], relOutTap);

		Flow flowRelationships = new LocalFlowConnector().connect(flowREL);
		// execute the flow, block until complete
		System.out.println("Finished Relationship Extraction");
		flowRelationships.writeDOT( "data/rel.dot" );

		// Flow to count named entities
		Pipe countPipe = new Pipe("CountPipe");
		countPipe = new Each(countPipe, new Debug());
		countPipe = new CountBy(countPipe, new Fields("word", "type"),
				new Fields("count"));


		FlowDef flowCNT = new FlowDef().setName("flowCount")
				.addSource(countPipe, finalOutTap)

				.addTailSink(countPipe, countOutTap);

		Flow flowCounts = new LocalFlowConnector().connect(flowCNT);
		System.out.println("Finished Named Entity Counts");

		CascadeConnector connector = new CascadeConnector();

		Cascade cascade = connector.connect( flowNamedEntities, flowRelationships, flowCounts );
		cascade.setFlowSkipStrategy(new FlowSkipIfSinkExists());
		cascade.complete();
		cascade.writeDOT("data/cascade.DOT");
		System.out.println("Done.");
	}
}
