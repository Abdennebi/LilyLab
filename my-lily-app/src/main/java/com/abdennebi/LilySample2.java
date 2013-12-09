package com.abdennebi;

import org.apache.zookeeper.KeeperException;
import org.joda.time.LocalDate;
import org.lilyproject.client.LilyClient;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.repo.PrintUtil;
import org.lilyproject.util.zookeeper.ZkConnectException;

import java.io.IOException;
import java.util.*;

import static org.lilyproject.repository.api.Scope.NON_VERSIONED;
import static org.lilyproject.repository.api.Scope.VERSIONED;
import static org.lilyproject.repository.api.Scope.VERSIONED_MUTABLE;

/**
 * http://docs.ngdata.com/lily-docs-current/g4/390-lily.html
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
        // (1) First we create a Record object. Again, this creates nothing in the repository yet, this is only a factory method, since Record is an interface.
        Record record = table.newRecord();

        // (2) We set the record type for the record. The second argument is the version, specified as a Long object. Setting it to null will cause the last version of the record type to be used, which is usually what you want. This argument is optional, shown here only to explain it, in further examples we will leave it off.
        record.setRecordType(new QName(BNS, "Book"));

        // (3) We set a field on the record. This is done by specifying its name and its value. The value argument is an Object, the actual type of value required depends on the value type of the field type.
        record.setField(new QName(BNS, "title"), "Lily, the definitive guide, 3rd edition");

        // (4) Create the record in the repository. The updated record is returned, which will contain the record ID and version assigned by the repository.
        record = table.create(record);

        // (5) We use the PrintUtil to dump the record to screen, the output is shown below.
        PrintUtil.print(record, repository);

    }

    void createRecordWithUserSpecifiedId(LTable table, LRepository repository, String BNS) throws RepositoryException, InterruptedException {
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");
        Record record = table.newRecord(id);
        record.setDefaultNamespace(BNS); // <= Use setDefaultNamespace to avoid QName
        record.setRecordType("Book");
        record.setField("title", "Lily, the definitive guide, 3rd edition");
        record = table.create(record);
        PrintUtil.print(record, repository);
    }

    /**
     * Updating a record consists of calling repository.update() with a record object of which the ID has been set to that of an existing record. If the record would not exist, a RecordNotFoundException will be thrown.
     * We use the repository.newRecord() method, even if what we are doing is updating an existing record. Remember that this method is used to instantiate a record object, not to create a record. When updating a record, you only need to set the fields in the record that you actually want to change. Fields that are not set will not be deleted, deleting fields is done by calling record.delete(fieldName, true) or record.addFieldsToDelete().
     * When updating a record, its record type will automatically move to the last version of the record type, unless you specify a specific version. The record type of each scope in which fields were modified will be set to this record type, in addition to the record type of the non-versioned scope which is always updated, since it is considered to be the reference record type.
     */
    void updateRecord(LTable table, LRepository repository, String BNS) throws RepositoryException, InterruptedException {
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");
        Record record = table.newRecord(id);
        record.setDefaultNamespace(BNS);
        record.setField("title", "Lily, the definitive guide, third edition");
        record.setField("pages", Long.valueOf(912));
        record.setField("manager", "Manager M");
        record = table.update(record);

        PrintUtil.print(record, repository);
    }

    void updateRecordViaRead(LTable table, LRepository repository, String BNS) throws RepositoryException, InterruptedException {
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");
        Record record = table.read(id);
        record.setDefaultNamespace(BNS);
        record.setField("released", new LocalDate());
        record.setField("authors", Arrays.asList("Author A", "Author B"));
        record.setField("review_status", "reviewed");
        record = table.update(record);

        PrintUtil.print(record, repository);
    }

    void readingRecord(LTable table, LRepository repository, String BNS) throws InterruptedException, RepositoryException {
        RecordId id = repository.getIdGenerator().newRecordId("lily-definitive-guide-3rd-edition");

        // (1)  If we just supply an ID when reading a record, the latest version of the record is fully read. The Record.getField() method returns the value of the field (here again, you could make use of setDefaultNamespace to avoid using the QName objects). The signature of this method declares a return type of Object, so you need to cast it to the expected type.
        Record record = table.read(id);
        String title = (String)record.getField(new QName(BNS, "title"));
        System.out.println(title);

        // (2)  We can specify a version number as second argument to read a specific version of the record
        record = table.read(id, 1L);
        System.out.println(record.getField(new QName(BNS, "title")));

        // (3)  It is also possible to read just the fields of the record that we are interested in. This way, the others do not need to be decoded and transported to us.
        record = table.read(id, 1L, new QName(BNS, "title"));
        System.out.println(record.getField(new QName(BNS, "title")));
    }

    /**
     * Creating a variant record is the same as creating a record, you just have to use an ID that contains variant properties.
     */
    void creatingVariant(LTable table, LRepository repository, String BNS) throws RepositoryException, InterruptedException {
        // (1)  We generate a master ID that we will use for the two variants.
        IdGenerator idGenerator = repository.getIdGenerator();
        RecordId masterId = idGenerator.newRecordId();

        // (2) We create the variant properties for the English language variant. This is simply a map.
        Map<String, String> variantProps = new HashMap<String, String>();
        variantProps.put("language", "en");

        // (3) We create the record ID for the English variant, consisting of the master record ID and the variant properties.
        RecordId enId = idGenerator.newRecordId(masterId, variantProps);

        // (4) We create the actual record.
        Record enRecord = table.newRecord(enId);
        enRecord.setRecordType(new QName(BNS, "Book"));
        enRecord.setField(new QName(BNS, "title"), "Car maintenance");
        enRecord = table.create(enRecord);

        // (5) We do the same for the Dutch language variant. Just as illustration, we get the master record ID by retrieving it from the English variant. A shortcut notation is used to create the variant properties map.
        RecordId nlId = idGenerator.newRecordId(enRecord.getId().getMaster(), Collections.singletonMap("language", "nl"));
        Record nlRecord = table.newRecord(nlId);
        nlRecord.setRecordType(new QName(BNS, "Book"));
        nlRecord.setField(new QName(BNS, "title"), "Wagen onderhoud");
        nlRecord = table.create(nlRecord);

        // (6) We use the getVariants method to get the list of all variants sharing the same master record ID, and print them out
        Set<RecordId> variants = table.getVariants(masterId);
        for (RecordId variant : variants) {
            System.out.println(variant);
        }
    }

    void linkFields(LTable table, LRepository repository, String BNS) throws RepositoryException, InterruptedException {
        // (1)
        Record record1 = table.newRecord();
        record1.setRecordType(new QName(BNS, "Book"));
        record1.setField(new QName(BNS, "title"), "Fishing 1");
        record1 = table.create(record1);

        // (2)
        Record record2 = table.newRecord();
        record2.setRecordType(new QName(BNS, "Book"));
        record2.setField(new QName(BNS, "title"), "Fishing 2");
        record2.setField(new QName(BNS, "sequel_to"), new Link(record1.getId()));
        record2 = table.create(record2);

        // (3)
        Link sequelToLink = (Link)record2.getField(new QName(BNS, "sequel_to"));
        RecordId sequelTo = sequelToLink.resolve(record2.getId(), repository.getIdGenerator());
        Record linkedRecord = table.read(sequelTo);
        System.out.println(linkedRecord.getField(new QName(BNS, "title")));
    }
}
