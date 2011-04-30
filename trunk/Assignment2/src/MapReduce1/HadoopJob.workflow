<?xml version="1.0" encoding="UTF-8" ?>

<hadoopjob>
  <bootstrap input="/Users/yarneo/Documents/input/in.txt" filesystem="00000000-0000-0000-377a-99710000002f"/>
  <configuration>
    <entry name="mapred.mapoutput.value.class" type="com.karmasphere.studio.common.lang.ClassDescriptor" value="MapReduce1.UserWritable"/>
    <entry name="mapred.output.value.class" type="com.karmasphere.studio.common.lang.ClassDescriptor" value="MapReduce1.UserWritable"/>
    <entry name="mapred.mapper.new-api" type="java.lang.Boolean" value="True"/>
    <entry name="mapred.output.key.class" type="com.karmasphere.studio.common.lang.ClassDescriptor" value="org.apache.hadoop.io.Text"/>
    <entry name="mapred.mapoutput.key.class" type="com.karmasphere.studio.common.lang.ClassDescriptor" value="org.apache.hadoop.io.Text"/>
    <entry name="karmasphere.integration.mapper.detecttypes" type="java.lang.Boolean" value="True"/>
    <entry name="karmasphere.integration.reducer.detecttypes" type="java.lang.Boolean" value="True"/>
    <entry name="mapred.reducer.new-api" type="java.lang.Boolean" value="True"/>
  </configuration>
  <operation type="input">
    <operator qualifiedName="org.apache.hadoop.mapreduce.lib.input.TextInputFormat" binaryName="org.apache.hadoop.mapreduce.lib.input.TextInputFormat"/>
  </operation>
  <operation type="mapper">
    <operator qualifiedName="MapReduce1.SubSequencesMapper" binaryName="MapReduce1.SubSequencesMapper"/>
  </operation>
  <operation type="partitioner">
    <operator qualifiedName="org.apache.hadoop.mapreduce.lib.partition.HashPartitioner" binaryName="org.apache.hadoop.mapreduce.lib.partition.HashPartitioner"/>
  </operation>
  <operation type="comparator">
    <operator qualifiedName="org.apache.hadoop.io.Text.Comparator" binaryName="org.apache.hadoop.io.Text$Comparator"/>
  </operation>
  <operation type="combiner">
  </operation>
  <operation type="reducer">
    <operator qualifiedName="MapReduce1.SubSequencesReducer" binaryName="MapReduce1.SubSequencesReducer"/>
  </operation>
  <operation type="outputformat">
    <operator qualifiedName="org.apache.hadoop.mapreduce.lib.output.TextOutputFormat" binaryName="org.apache.hadoop.mapreduce.lib.output.TextOutputFormat"/>
  </operation>
  <defaultArgs>
  </defaultArgs>
  <uploadedFiles>
  </uploadedFiles>
</hadoopjob>
