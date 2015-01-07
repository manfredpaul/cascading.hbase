/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.hbase;

import java.io.IOException;

import cascading.flow.Flow;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import static org.junit.Assert.assertEquals;

public abstract class HBaseTestsStaticScheme extends HBaseTests
  {

  protected void verifySink( Flow flow, int expects ) throws IOException
    {
    int count = 0;

    TupleEntryIterator iterator = flow.openSink();

    while( iterator.hasNext() )
      {
      count++;
      System.out.println( "iterator.next() = " + iterator.next() );
      }

    iterator.close();

    assertEquals( "wrong number of values in " + flow.getSink().toString(),
      expects, count );
    }

  protected void verify( String tableName, String family, String charCol, int expected ) throws IOException
    {
    byte[] familyBytes = Bytes.toBytes( family );
    byte[] qualifierBytes = Bytes.toBytes( charCol );

    HTable table = new HTable( configuration, tableName );
    ResultScanner scanner = table.getScanner( familyBytes, qualifierBytes );

    int count = 0;
    for( Result rowResult : scanner )
      {
      count++;
      }
    scanner.close();
    table.close();

    assertEquals( "wrong number of rows", expected, count );
    }
  }
