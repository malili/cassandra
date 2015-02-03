/*
 * Copyright (c) 2013-2014 Shandong Antrol Network Technology Co.Ltd. All rights reserved. 版权所有(c) 2013-2014
 * 山东蚁巡网络科技有限公司。保留所有权利。
 */
package com.antrol.cassandra;

import java.util.Iterator;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.Drop;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;

/**
 * <p>功能描述,该部分必须以中文句号结尾。<p>
 * 
 * <dependency> <groupId>com.datastax.cassandra</groupId> <artifactId>cassandra-driver-core</artifactId>
 * <version>2.0.1</version> </dependency>
 * 
 * 创建日期 2015年2月3日<br>
 * @author $Author$<br>
 * @version $Revision$ $Date$
 * @since 3.0.0
 */
public class SimpleClient {
    private Cluster cluster;
    private Session session;

    public void connect(String node) {
        // 构建一个集群对象，”192.168.22.161″ 是cassandra的种子节点(seed node).
        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
                    host.getRack());
        }
        // 创建应用的唯一session
        session = cluster.connect();
        // 针对一个特定的keyspace获取一个session
        // session是线程安全的，所以一个应用中，你可以只有一个session实例，官方建议一个keyspace一个session。
        // Session session = cluster.connect("mykeyspace");
        String cql = "select * from mykeyspace.tablename;";
        // session可以直接支持执行cql语句。你完全可以用这种方式完成任意操作，记住cql语句后面一定要带分号。
        session.execute(cql);
        // 如果你不想繁琐的去拼字符串，你可以用com.datastax.driver.core.Querybuilder。
        // insert 一条记录String cql = “insert into mykeyspace.tablename(a,b) values(1,2);”
        session.execute(QueryBuilder.insertInto("mykeyspace", "tablename").values(new String[] {"a", "b" },
                new Object[] {1, 2 }));
        // delete 记录String cql = “delete from mykeyspace.tablename where a=1″;
        session.execute(QueryBuilder.delete().from("mykeyspace", "tablename").where(QueryBuilder.eq("a", 1)));
        // update 记录String cql = “update mykeyspace.tablename set b=2 where a=1″
        session.execute(QueryBuilder.update("mykeyspace", "tablename").with(QueryBuilder.set("b", 2))
                .where(QueryBuilder.eq("a", 1)));
        // select 记录String cql = “select a, b from mykeyscpace.tablename where a>1 and b<0″
        ResultSet result = session.execute(QueryBuilder.select("a", "b").from("mykeyspace", "tablename")
                .where(QueryBuilder.gt("a", 1)).and(QueryBuilder.lt("b", 0)));
        Iterator<Row> iterator = result.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            row.getInt("a");
            row.getInt("b");
        }
        // 可以像jdbc那样使用预编译占位符的方式
        BoundStatement bindStatement = session.prepare("select * from mykeyspace.tablename where a=? and b=?").bind(
                "1", "2");
        session.execute(bindStatement);
        // 或者
        // PreparedStatement prepareStatement = session.prepare("select * from mykeyspace.tablename where a=? and b=?");
        // BoundStatement bindStatement = new BoundStatement(prepareStatement).bind("1", "2");
        // session.execute(bindStatement);
        // 或者
        Insert insert = QueryBuilder.insertInto("mykeyspace", "tablename").values(new String[] {"a", "b" },
                new Object[] {QueryBuilder.bindMarker(), QueryBuilder.bindMarker() });
        // BoundStatement bindStatement = new BoundStatement(session.prepare(insert)).bind("1", "2");
        // session.execute(bindStatement);
        // 批量batch的方式也有的。
        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(insert);
        batchStatement.add(bindStatement);
        session.execute(batchStatement);
    }

    public void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        SimpleClient client = new SimpleClient();
        client.connect("192.168.2.14");
        client.close();
    }

    public void test() {
        // 在cassanda的官方驱动cassandra-driver-core-2.1.3.jar之前，创建表、修改表、创建索引操作，只能通过拼CQL语句，
        // 然后通过session去执行的方式。可能会经常导致语法格式错误。
        // 在最新版的驱动cassandra-driver-core-2.1.3.jar中，提供了一种更方便的对表的修改方式。
        // 类似于用于增删改查操作的com.datastax.driver.core.querybuilder.QueryBuilder类，
        // 它提供了一个com.datastax.driver.core.schemabuilder.SchemaBuilder类用于对表的操作。
        // 创建表
        Create createTbale = SchemaBuilder.createTable("mykeyspace", "mytable").addPartitionKey("pk1", DataType.cint())
                .addColumn("col1", DataType.text()).addColumn("col2", DataType.bigint());
        session.execute(createTbale);
        // 增加一列
        SchemaStatement addColumn = SchemaBuilder.alterTable("mykeyspace", "mytable").addColumn("col3")
                .type(DataType.cdouble());
        session.execute(addColumn);
        // 删除一列
        SchemaStatement dropColumn = SchemaBuilder.alterTable("mykeyspace", "mytable").dropColumn("col2");
        session.execute(dropColumn);
        // 修改一列
        SchemaStatement alterColumn = SchemaBuilder.alterTable("mykeyspace", "mytable").alterColumn("col1")
                .type(DataType.cdouble());
        session.execute(alterColumn);
        // 列更改名字
        SchemaStatement renameColumn = SchemaBuilder.alterTable("mykeyspace", "mytable").renameColumn("col1")
                .to("col4");
        session.execute(renameColumn);
        // 增加索引
        SchemaStatement createIndex = SchemaBuilder.createIndex("idx_col4").onTable("mykeyspace", "mytable")
                .andColumn("col4");
        session.execute(createIndex);
        // 删除索引
        Drop dropIndex = SchemaBuilder.dropIndex("mykeyspace", "idx_col4").ifExists();
        session.execute(dropIndex);
        // 删除表
        Drop dropTable = SchemaBuilder.dropTable("mykeyspace", "mytable").ifExists();
        session.execute(dropTable);
    }
}
