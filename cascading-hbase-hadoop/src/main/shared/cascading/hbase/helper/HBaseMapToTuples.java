/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.hbase.helper;

import java.util.Map.Entry;
import java.util.NavigableMap;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseMapToTuples extends BaseOperation<Void> implements Function<Void>
  {

  Fields inputFields;

  public HBaseMapToTuples( Fields declaredFields, Fields inputFields )
    {
    super( 2, declaredFields );
    this.inputFields = inputFields;
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Void> functionCall )
    {
    String row = functionCall.getArguments().getString( inputFields.get( 0 ) );
    @SuppressWarnings("unchecked")
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> keyValueMaps = (NavigableMap<byte[], NavigableMap<byte[], byte[]>>) functionCall
      .getArguments().getObject( inputFields.get( 1 ) );

    for( Entry<byte[], NavigableMap<byte[], byte[]>> keyValue : keyValueMaps.entrySet() )
      {
      for( Entry<byte[], byte[]> value : keyValue.getValue().entrySet() )
        {
        functionCall.getOutputCollector().add(
          new Tuple( row, Bytes.toString( keyValue.getKey() ), Bytes.toString( value.getKey() ), Bytes.toString( value.getValue() ) ) );
        }
      }
    }

  }
