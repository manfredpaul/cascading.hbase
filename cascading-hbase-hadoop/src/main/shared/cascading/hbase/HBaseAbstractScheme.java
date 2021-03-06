package cascading.hbase;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.hbase.helper.TableInputFormat;
import cascading.scheme.Scheme;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

public abstract class HBaseAbstractScheme extends Scheme<Configuration, RecordReader, OutputCollector, Object[], Object[]>
  {
  /** Field keyFields */
  protected Fields keyField;

  protected void validate()
    {
    if( keyField.size() != 1 )
      throw new IllegalArgumentException( "may only have one key field, found: " + keyField.print() );
    }

  protected void setSourceSink( Fields keyFields, Fields... columnFields )
    {
    Fields allFields = Fields.join( keyFields, Fields.join( columnFields ) ); // prepend

    setSourceFields( allFields );
    setSinkFields( allFields );
    }

  protected void setSourceInitFields( Configuration conf, String columns )
    {
    conf.set( "mapred.mapper.new-api", "false");
    conf.set( "mapred.input.format.class", TableInputFormat.class.getName() );
    conf.set( TableInputFormat.SCAN_COLUMNS, columns );
    }

  protected void setSinkInitFields( Configuration conf )
    {
    conf.set( "mapred.output.format.class", TableOutputFormat.class.getName() );

    conf.set( "mapred.mapoutput.key.class", ImmutableBytesWritable.class.getName() );
    conf.set( "mapred.mapoutput.value.class", Put.class.getName() );

    conf.set( "mapred.mapper.new-api", "false");
    conf.set( "mapred.reducer.new-api", "false");
    }

  protected Tuple sourceGetTuple( Object key )
    {
    Tuple result = new Tuple();

    ImmutableBytesWritable keyWritable = (ImmutableBytesWritable) key;
    result.add( Bytes.toString( keyWritable.get() ) );

    return result;
    }

  protected Put sinkGetPut( TupleEntry tupleEntry )
    {
    Tuple keyTuple = tupleEntry.selectTuple( keyField );

    byte[] keyBytes = Bytes.toBytes( keyTuple.getString( 0 ) );

    return new Put( keyBytes );
    }

  public abstract String[] getFamilyNames();

  @Override
  public void sourcePrepare( FlowProcess<? extends Configuration> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
    Object[] pair = new Object[]{sourceCall.getInput().createKey(), sourceCall.getInput().createValue()};

    sourceCall.setContext( pair );
    }

  @Override
  public void sourceCleanup( FlowProcess<? extends Configuration> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
    {
    sourceCall.setContext( null );
    }

  }
