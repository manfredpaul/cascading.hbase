# Welcome #

 This is the Cascading.HBase module.

 It provides support for reading/writing data to/from an HBase
 cluster when bound to a Cascading data processing flow.

 Cascading is a feature rich API for defining and executing complex,
 scale-free, and fault tolerant data processing workflows on a Hadoop
 cluster. It can be found at the following location:

   http://www.cascading.org/

 HBase is the Hadoop database. Its an open-source, distributed,
 column-oriented store modeled after the Google paper on Bigtable.

   http://hbase.apache.org/

# History #

 This version has roots from the original Cascading.HBase effort by Chris
 Wensel, and then modified by Kurt Harriger to add the dynamic scheme, putting
 tuple fields into HBase columns, and vice versa.  Twitter's Maple project also
 has roots from the original Cascading.HBase project, but is an update to
 Cascading 2.0.  Maple lacks the dynamic scheme, so this project basically
 combines everything before it and updates to Cascading 2.2.x and HBase 0.94.x.
 It also adds support for lingual.

# Building #

 This version could be built by using gradle:

     > gradle build

If the tests are failing on your machine, do a `umask 022` before starting the
build.

# Using #

## Hadoop 1 vs. Hadoop 2

HBase provides two sets of dependencies since version 0.96, to work with the two
different major version os hadoop. This builds supports both versions and
creates two sets of jars.

If you are using a Hadoop distribution based on Apache Hadoop 1.x, you have to
use `cascading:cascading-hbase-hadoop:2.5.0-*`. If you are using a Hadoop 2.x
based distribution, you have to use
`cascading:cascading-hbase-hadoop2-mr1:2.5.0-+` as a dependency.

## In cascading applications ##

Add the correct jar for your distribution to the classpath with your build tool
of choice.

All jars are deployed on [conjars](http://conjars.org/).

See the `HBaseDynamicTest` and `HBaseStaticTest` unit tests for sample code on
using the HBase taps, schemes and helpers in your Cascading application.

## In lingual ##

This project also creates a lingual compliant provider jars, which can be used
to talk to HBase via standard SQL. Below you can find a possible session with
lingual and HBase.

    # the hbase provider can only be used with the hadoop or hadoop2-mr1 platform
    > export LINGUAL_PLATFORM=hadoop

    # tell lingual, where the namenode, the jobtracker and the hbase zk quorum are
    > export LINGUAL_CONFIG=fs.default.name=hdfs://master.local:9000,mapred.job.tracker=master.local:9001,hbase.zookeeper.quorum=hadoop1.local


First we install the provider, by downloading it from conjars.

    > lingual catalog --provider --add cascading:cascading-hbase-hadoop:2.5.0-+:provider
or
    > lingual catalog --provider --add cascading:cascading-hbase-hadoop2-mr1:2.5.0-+:provider

Next we are creating a new schema called `working` to work with.

    > lingual catalog --schema working --add

Now we add the `hbase` format from the `hbase` provider to the schema. We tell
the provider, which column family, we want to work with. In this case, the
family is called `cf`. Please note that there is a strict one-to-one mapping
from HBase column families to lingual tables. If you want to access multiple
column families in HBase from lingual, you can map them to different tables.

    > lingual catalog --schema working --format hbase --add  --properties=family=cf --provider hbase

We register a new stereotype called `hbtest` with four fields: `ROWKEY`, `A`,
`B` and `C` all of type `string`. These are the fields that will be used by the
HBaseTap. The first field is always used as the rowkey in the table. All
subsequent fields are used as qualifiers in a given column family (see above).

    > lingual catalog --schema working --stereotype hbtest -add --columns ROWKEY,A,B,C --types string,string,string,string

Now we add the `hbase` protocol to the schema.

    > lingual catalog --schema working --protocol hbase --add --provider hbase

Finally we create a lingua table called `hb` with the hbase provider. The table
is called `cascading` in the HBase instance, so we use that as the identifier.

    > lingual catalog --schema working --table hb --stereotype hbtest -add "cascading" --protocol hbase --format hbase --provider hbase

Now we can talk to the HBase table from lingual:

    > lingual shell
    (lingual)> select * from "working"."hb";
    +---------+----+----+----+
    | ROWKEY  | A  | B  | C  |
    +---------+----+----+----+
    +---------+----+----+----+

    (lingual)> insert into "working"."hb" values ('42', 'one', 'two', 'three');
    +-----------+
    | ROWCOUNT  |
    +-----------+
    | 1         |
    +-----------+

    (lingual)> select * from "working"."hb";
    +---------+------+------+--------+
    | ROWKEY  |  A   |  B   |   C    |
    +---------+------+------+--------+
    | 42      | one  | two  | three  |
    +---------+------+------+--------+


### Limitations ###

As explained above only qualifiers from one column family can be mapped to the
same table in lingual. You can still map multiple column families in the same
HBase table to multiple tables in lingual.

Next to that, there is currently a limitation related to the casing of the
qualifiers in a column family. Since lingual uses SQL semantics for column
names, it tries to normalize them and uses upper case names. You can use
lowercase names as well, but you might run into problems, when you do a
`select * from table` style query. This limitation might be removed in future versions
of lingual and therefore in the HBase provider.  The easiest way to work around
this limitation is using uppercase qualifiers.

Last but not least keep in mind that this provider gives you a SQL interface to
HBase, but this interface is not meant for realtime queries. In the spirit of
lingual it is meant as a SQL driven way for batch processing.

### Types, Lingual, and HBase

The lingual provider takes a pragmatic approach to types, when it reads and
writes to HBase. Since HBase has no type enforcement and there is no reliable
way to guess the types, the provider converts every field to a `String` before
storing it. The type conversion is done via the types on the Fields instance.
If the type is a `CoercibleType`, the coerce method is called.  When the data is
read back, the is converted to its canonical representation, before it handed
back to lingual. This makes sure that data written from lingual can be read back
in lingual.

Other systems interacting with the same table need to take his behaviour into
account.


# Acknowledgements #

The code contains contributions from the following authors:
- Andre Kelpe
- Brad Anderson
- Chris K Wensel
- Dave White
- Dru Jensen
- Jean-Daniel Cryans
- JingYu Huang
- Ken MacInnis
- Kurt Harriger
- matan
- Ryan Rawson
- Soren Macbeth

