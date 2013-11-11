package com.abdennebi;

import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.repo.PrintUtil;
import org.lilyproject.util.zookeeper.ZkConnectException;

import java.io.IOException;

import static org.lilyproject.repository.api.Scope.NON_VERSIONED;
import static org.lilyproject.repository.api.Scope.VERSIONED;
import static org.lilyproject.repository.api.Scope.VERSIONED_MUTABLE;

/**
 * Created with IntelliJ IDEA.
 * User: abdennebi
 * Date: 09/11/13
 * Time: 18:28
 * To change this template use File | Settings | File Templates.
 */
public class LilySample2 {

    public void test() throws InterruptedException, RepositoryException, ZkConnectException, KeeperException, IOException {

        LilyClient lilyClient = new LilyClient("localhost:2181", 20000);

        LRepository repository = lilyClient.getDefaultRepository();

        TypeManager typeManager = repository.getTypeManager();

        String BNS = "book";

        LTable table = repository.getDefaultTable();

        // Creating a record type

        // (1) Get a reference to the value type we want to use for our field. The value type is specified as string (rather than an enum or so), as there are lots of variations possible and the value types are designed to be extensible.
        ValueType stringValueType = typeManager.getValueType("STRING");

        // (2) Create the field type object. Since FieldType is an interface, we cannot instantiate it directly. We want to keep our code implementation-independent, therefore we will not directly instantiate an implementation but use the factory method newFieldType(). This method does nothing more than instantiating a field type object, at this point nothing changes yet in the repository. The same holds for all methods in the API that start with "new".
        QName titleQN = new QName(BNS, "title");
        FieldType title = typeManager.newFieldType(stringValueType, titleQN, VERSIONED);
        FieldType description = typeManager.createFieldType("BLOB", new QName(BNS, "description"), VERSIONED);
        FieldType authors = typeManager.createFieldType("LIST<STRING>", new QName(BNS, "authors"), VERSIONED);
        FieldType released = typeManager.createFieldType("DATE", new QName(BNS, "released"), VERSIONED);
        FieldType pages = typeManager.createFieldType("LONG", new QName(BNS, "pages"), VERSIONED);
        FieldType sequelTo = typeManager.createFieldType("LINK", new QName(BNS, "sequel_to"), VERSIONED);
        FieldType manager = typeManager.createFieldType("STRING", new QName(BNS, "manager"), NON_VERSIONED);
        FieldType reviewStatus = typeManager.createFieldType("STRING", new QName(BNS, "review_status"), VERSIONED_MUTABLE);

        // (3) Create the field type in the repository. The updated field type object, in which in this case the ID of the field type will be assigned, is returned by this method.
        title = typeManager.createFieldType(title);

        // (4) Create the record type object. This is pretty much the same as step (2): it creates an object, but nothing yet in the repository. The field type is added to the record type. The boolean argument specifies if the field type is mandatory
        final QName bookQN = new QName(BNS, "Book");
        RecordType book = typeManager.newRecordType(bookQN);
        book.addFieldTypeEntry(title.getId(), true);

        // (5) Create the record type. Similar to point (3), here the record type is actually created in the repository, the updated record type object is returned.
        book = typeManager.createRecordType(book);

        // (6) The PrintUtil class is used to dump the record type to screen, its output is shown below.
        PrintUtil.print(book, repository);


        book = typeManager.getRecordTypeByName(new QName(BNS, "Book"), null);

            // The order in which fields are added does not matter
        book.addFieldTypeEntry(description.getId(), false);
        book.addFieldTypeEntry(authors.getId(), false);
        book.addFieldTypeEntry(released.getId(), false);
        book.addFieldTypeEntry(pages.getId(), false);
        book.addFieldTypeEntry(sequelTo.getId(), false);
        book.addFieldTypeEntry(manager.getId(), false);
        book.addFieldTypeEntry(reviewStatus.getId(), false);

// Now we call updateRecordType instead of createRecordType
        book = typeManager.updateRecordType(book);

         // Creating a record

    }

    void createRecord(LTable table, LRepository repository, String BNS ) throws RepositoryException, InterruptedException {
        // (1)
        Record record = table.newRecord();

        // (2)
        record.setRecordType(new QName(BNS, "Book"));

        // (3)
        record.setField(new QName(BNS, "title"), "Lily, the definitive guide, 3rd edition");

        // (4)
        record = table.create(record);

        // (5)
        PrintUtil.print(record, repository);


    }
}
