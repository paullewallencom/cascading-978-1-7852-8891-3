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

package com.ai.learning;

import java.util.ArrayList;
import java.util.Iterator;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
/*
 * This Cascading Buffer is used to extract relationships among the named entities within a sentence
 * If two or more named entities occur within one sentence - the are related
 */
public class RelationshipBuffer<Context> extends BaseOperation<Context> implements Buffer<Context>
{
	static Fields fieldOutput = new Fields("documentname","sentnumber", "word1","type1","word2","type2");
	static Fields fieldDecraration = new Fields( "documentname","sentnumber", "word", "type");

	public RelationshipBuffer() {
		super(4, fieldOutput);
	}
	public void operate( FlowProcess flowProcess, BufferCall bufferCall )
	{

		TupleEntry group = bufferCall.getGroup();

		// get all the current argument values for this grouping
		Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
		
		// create a Tuple to hold our result values and set its document name field
		Tuple result = Tuple.size(6);
		
		ArrayList<NamedEntity> entities = new ArrayList<NamedEntity>();

		int name_count = 0;
		while( arguments.hasNext() )
		{
			Tuple tuple = arguments.next().getTuple();
			NamedEntity tempEntity = new NamedEntity();
			tempEntity.setName(tuple.getString(2));
			tempEntity.setType(tuple.getString(3));
			tempEntity.setDoc(tuple.getString(0));
			entities.add(tempEntity);
			name_count++;
		}

		for (int i=0;i<name_count;i++)
			for (int j=i+1; j<name_count; j++)
			{
				result.set(0, group.getString(0)); // document name
				result.set(1, group.getString(1)); // sentence number
				result.set(2, entities.get(i).getName()); // head of relationship entity
				result.set(3, entities.get(i).getType()); // type
				result.set(4, entities.get(j).getName()); // other relationship entity
				result.set(5, entities.get(j).getType()); //type
				bufferCall.getOutputCollector().add( result );
			}

	}
}
