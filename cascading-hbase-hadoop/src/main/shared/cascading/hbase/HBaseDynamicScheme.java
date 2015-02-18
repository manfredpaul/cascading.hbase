package cascading.hbase;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

public class HBaseDynamicScheme extends HBaseAbstractScheme
  {
  private Fields valueField;
  private String[] familyNames;

  public HBaseDynamicScheme( Fields keyField, Fields valueField, String... familyNames )
    {
    setSourceSink( keyField, valueField );

    this.familyNames = familyNames;
    this.keyField = keyField;
    this.valueField = valueField;
    this.familyNames = familyNames;

    validate();

    if( valueField.size() != 1 )
      throw new IllegalArgumentException( "may only have one value field, found: " + valueField.print() );
    }

  private String getTableFromTap( HBaseTap tap )
    {
    Path tapPath = tap.getPath();
    String tapPathStr = tapPath.toString();
    // TODO: redefine exception
    return tapPathStr.split( "://" )[ 1 ];
    }

  @Override
  public String[] getFamilyNames()
    {
    return familyNames;
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf )
    {
    setSourceInitFields( conf, " " );
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf )
    {
    setSinkInitFields( conf );
    conf.set( TableOutputFormat.OUTPUT_TABLE, getTableFromTap( (HBaseTap) tap ) );
    }

  @Override
  public boolean source( FlowProcess<? extends Configuration> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
    Object key = sourceCall.getContext()[ 0 ];
    Object value = sourceCall.getContext()[ 1 ];
    boolean hasNext = sourceCall.getInput().next( key, value );

    if( !hasNext )
      return false;

    Tuple result = sourceGetTuple( key );
    Result row = (Result) value;
    result.add( row.getNoVersionMap() );
    sourceCall.getIncomingEntry().setTuple( result );

    return true;
    }

  @Override
  public void sink( FlowProcess<? extends Configuration> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
    {
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();

    Put put = sinkGetPut( tupleEntry );

    Tuple valueTuple = tupleEntry.selectTuple( valueField );
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> values =
      (NavigableMap<byte[], NavigableMap<byte[], byte[]>>) valueTuple.getObject( 0 );

    for( Entry<byte[], NavigableMap<byte[], byte[]>> keyValue : values
      .entrySet() )
      {
      for( Entry<byte[], byte[]> value : keyValue.getValue().entrySet() )
        {
        put.add(
          nullValue( keyValue.getKey() ),
          nullValue( value.getKey() ),
          nullValue( value.getValue() )
        );
        }
      }

    OutputCollector collector = sinkCall.getOutput();
    collector.collect( null, put );
    }

  private byte[] nullValue( byte[] in )
    {
    if( null == in )
      return HConstants.EMPTY_BYTE_ARRAY;
    else
      return in;
    }
  }
