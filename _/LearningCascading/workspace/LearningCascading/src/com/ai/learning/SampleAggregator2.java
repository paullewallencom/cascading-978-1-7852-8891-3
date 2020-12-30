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
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/*
 * This is a more stable version of SampleAggregator which uses Prepare method. Given merchandise purchased and returned by store,
 * calculate total merchandise kept.
 */

public class SampleAggregator2 extends BaseOperation<SampleAggregator2.Context> implements Aggregator<SampleAggregator2.Context>
  {
  public static class Context
  {
    long value = 0;
    Tuple tuple = Tuple.size(2);
  }

  public SampleAggregator2()
  {
    super(2, new Fields("total_kept"));
  }

  public SampleAggregator2( Fields calcFields )
  {
    // expects 2 argument, fail otherwise
    super( 2, calcFields );
  }
  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<SampleAggregator2.Context> aggregatorCall)
  {
	  aggregatorCall.setContext(new SampleAggregator2.Context());
  }
  @Override
  public void cleanup(FlowProcess flowProcess, OperationCall<SampleAggregator2.Context> aggregatorCall)
  {
	   aggregatorCall.setContext(null); 
  }
  public void start( FlowProcess flowProcess, AggregatorCall<SampleAggregator2.Context> aggregatorCall )
  {
      // set the context object,
	  Context context = aggregatorCall.getContext() ;
      context.value = 0;
      context.tuple.clear();
  }

  public void aggregate( FlowProcess flowProcess, AggregatorCall<SampleAggregator2.Context> aggregatorCall )
  {
      TupleEntry arguments = aggregatorCall.getArguments();
      Context context = aggregatorCall.getContext();

      // add the current argument value to the current sum
      context.value += (arguments.getInteger(0)-arguments.getInteger(1));
   }

  public void complete( FlowProcess flowProcess, AggregatorCall<SampleAggregator2.Context> aggregatorCall )
  {
	  Context context = aggregatorCall.getContext();

	  // create a Tuple to hold our result values
	  Tuple result = context.tuple;

	  // set the sum
	  result.add( context.value );

	  // return the result Tuple
	  aggregatorCall.getOutputCollector().add( result );
  }
}