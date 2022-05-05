from pyspark.python.pyspark.shell import sqlContext

df = sqlContext.read.format('org.apache.hadoop.hbase.spark') \
    .option('hbase.table','books') \
    .option('hbase.columns.mapping', \
            'title STRING :key, \
            author STRING info:author, \
            year STRING info:year, \
            views STRING analytics:views') \
    .option('hbase.use.hbase.context', False) \
    .option('hbase.config.resources', 'file:///etc/hbase/conf/hbase-site.xml') \
    .option('hbase-push.down.column.filter', False) \
    .load()

df.show()