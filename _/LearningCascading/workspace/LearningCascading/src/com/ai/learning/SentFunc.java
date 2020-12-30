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
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/*
 * This Cascading function, given a document, returns a tuple stream that consists of individual
 * sentences. Uses Utilities.getSentences utility function
 */
public class SentFunc extends BaseOperation<Tuple> implements Function<Tuple> 
{
   static Fields fieldDeclaration = new Fields( "document","text");
   static Fields fieldOutput = new Fields( "document","sentnum", "sentence");
   
   public SentFunc () {
           super(2, fieldOutput);
    }
     
   @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Tuple> call )
    {
         // create a reusable Tuple of size 3
         call.setContext( Tuple.size(3) );
    }
 
	@Override
	public void operate( FlowProcess flowProcess, FunctionCall<Tuple> call )
	{
	    // get the arguments TupleEntry
	    TupleEntry arguments = call.getArguments();
	    String sentences[] = Utilities.getSentences (arguments.getString(1)); 
	    int sentCounter=0;
	    for ( String sent: sentences)
	   {
	             // get our previously created Tuple
	              Tuple result = call.getContext(); 
	    		
	             // First field is document name
	             result.set(0,arguments.getString(0));
	              // Second field is sentence number
	             result.set(1,sentCounter);
	            // Third field is the sentence
	             result.set(2,sent);
	             // return the result tuple
	              call.getOutputCollector().add( result );
	             sentCounter++;
	    }
	}

    @Override
	 public void cleanup( FlowProcess flowProcess, OperationCall<Tuple> call )
	 {
	    call.setContext( null );
	 }
}
