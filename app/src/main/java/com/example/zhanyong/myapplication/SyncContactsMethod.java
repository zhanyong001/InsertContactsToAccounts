package com.android.contacts.account;

import android.content.ContentProvider;
import android.content.ContentProviderOperation;
import android.content.ContentProviderResult;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.OperationApplicationException;
import android.database.Cursor;
import android.net.Uri;
import android.os.RemoteException;
import android.provider.ContactsContract;
import android.provider.ContactsContract.CommonDataKinds.Email;
import android.provider.ContactsContract.CommonDataKinds.Event;
import android.provider.ContactsContract.CommonDataKinds.GroupMembership;
import android.provider.ContactsContract.CommonDataKinds.Im;
import android.provider.ContactsContract.CommonDataKinds.Note;
import android.provider.ContactsContract.CommonDataKinds.Organization;
import android.provider.ContactsContract.CommonDataKinds.Phone;
import android.provider.ContactsContract.CommonDataKinds.Photo;
import android.provider.ContactsContract.CommonDataKinds.StructuredName;
import android.provider.ContactsContract.CommonDataKinds.StructuredPostal;
import android.provider.ContactsContract.CommonDataKinds.Website;
import android.provider.ContactsContract.CommonDataKinds.SipAddress;
import android.provider.ContactsContract.Data;
import android.provider.ContactsContract.RawContacts;
import android.provider.MzContactsContract;
import android.provider.MzDialerProviderContract;
import android.text.TextUtils;
import android.util.Log;
import android.util.LongSparseArray;

import com.android.common.io.MoreCloseables;
import com.android.contacts.MzGroupMetaDataLoad;
import com.android.contacts.common.model.account.AccountWithDataSet;
import com.android.contacts.group.GroupAccountInfo;
import com.meizu.contacts.common.util.GroupUtils;
import com.meizu.contacts.db.MzConstants;
import com.meizu.contacts.db.MzConstants.Dialer.MzMimetype;
import com.meizu.contacts.db.aking.AkingContract.MzAccount;
import com.meizu.contacts.db.aking.AkingContract.MzData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by zhanyong on 17-12-8.
 */

public class MzSyncContactsMethod {

    private static final String TAG = "SyncAccountService";

    private SyncAccountService mService;
    private ContentResolver mResolver;
    private List<AccountWithDataSet> mSaveSyncAccounts = new ArrayList<>();
    private ArrayList<Long> mAllContactIds = new ArrayList<>();
    private HashMap<String, ArrayList<GroupAccountInfo>> mTitleGroupAccountsKey = new HashMap<>();
    private HashMap<String, HashMap<AccountWithDataSet, Long>> mInsertTitleAccountKey = new HashMap<>();

    private String mCustomRingTone = null;

    private ArrayList<ContentProviderOperation> mRemoteOperationList;

    private static final int MAX_CURSOR_NUMBER = 250;

    private ArrayList<NameData> mNameList = new ArrayList<>();
    private ArrayList<PhoneData> mPhoneList = new ArrayList<>();
    private ArrayList<OrganizationData> mOrganizationList = new ArrayList<>();
    private ArrayList<PostalData> mPostalList = new ArrayList<>();
    private ArrayList<EmailData> mEmailList = new ArrayList<>();
    private ArrayList<PhotoData> mPhotoList = new ArrayList<>();
    private ArrayList<EventData> mEventList = new ArrayList<>();
    private ArrayList<ImData> mImList = new ArrayList<>();
    private ArrayList<NoteData> mNoteList = new ArrayList<>();
    private ArrayList<WebsiteData> mWebsiteList = new ArrayList<>();
//    private ArrayList<GroupData> mGroupList = new ArrayList<>();

    private int mCount = 0;
    private static final int MAX_APPLY_BATCH = 500;

    public MzSyncContactsMethod(SyncAccountService server,
                                ArrayList<Long> contactIds,
                                List<AccountWithDataSet> saveAccounts) {
        mService = server;
        mAllContactIds = contactIds;
        mResolver = server.getContentResolver();
        mSaveSyncAccounts = saveAccounts;
    }

    private static class DataQuery {
        static final String[] DATA_COLUMNS = new String[]{
                MzData.CONTACT_ID,
                MzData.RAW_CONTACT_ID,
                MzData.DATA1,
                MzData.DATA2,
                MzData.DATA3,
                MzData.DATA4,
                MzData.DATA5,
                MzData.DATA6,
                MzData.DATA7,
                MzData.DATA8,
                MzData.DATA9,
                MzData.DATA10,
                MzData.DATA11,
                MzData.DATA12,
                MzData.DATA13,
                MzData.DATA14,
                MzData.DATA15,
                MzData.IS_PRIMARY,
                MzData.IS_SUPER_PRIMARY,
                MzMimetype.MIMETYPE,
                MzData.CUSTOM_RINGTONE,
                MzData.PHOTO_ID,
                MzData.PHOTO_URI,
                MzData.LOOKUP_KEY,
                MzData.MODIFY_TIME,
                MzData.DELETED,
                MzAccount.ACCOUNT_NAME,
                MzAccount.ACCOUNT_TYPE,
                MzAccount.DATA_SET,
                MzData._ID,
                MzData.MODIFY_KEY,
                MzData.SYNC_KEY,
        };

        public static final int CONTACT_ID = 0;
        public static final int RAW_CONTACT_ID = 1;
        public static final int DATA1 = 2;
        public static final int DATA2 = 3;
        public static final int DATA3 = 4;
        public static final int DATA4 = 5;
        public static final int DATA5 = 6;
        public static final int DATA6 = 7;
        public static final int DATA7 = 8;
        public static final int DATA8 = 9;
        public static final int DATA9 = 10;
        public static final int DATA10 = 11;
        public static final int DATA11 = 12;
        public static final int DATA12 = 13;
        public static final int DATA13 = 14;
        public static final int DATA14 = 15;
        public static final int DATA15 = 16;
        public static final int IS_PRIMARY = 17;
        public static final int IS_SUPERPRIMARY = 18;
        public static final int MIMETYPE = 19;
        public static final int CUSTOM_RINGTONE = 20;
        public static final int PHOTO_ID = 21;
        public static final int PHOTO_URI = 22;
        public static final int DATA_LOOK_UP_KEY = 23;
        public static final int MODIFY_TIME = 24;
        public static final int DELETED = 25;
        public static final int ACCOUNT_NAME = 26;
        public static final int ACCOUNT_TYPE = 27;
        public static final int DATA_SET = 28;
        public static final int DATA_ID = 29;
        public static final int MODIFY_KEY = 30;
        public static final int SYNC_KEY = 31;
    }

    public void startSync() {
        if (mAllContactIds.size() == 0 || mResolver == null || mService == null || mSaveSyncAccounts.size() < 2) {
            Log.d(TAG, "mContactIds or mResolver or mService is null and mSaveSyncAccounts.size: " + mSaveSyncAccounts.size());
            return;
        }
        //get All Group Message
        //queryAndSyncGroup();

        //account sync
        averageContactCount(mAllContactIds, false);
        if (mService != null) {
            mService.changeSyncState(true, mCount > 0);
        }
        dealWithOperationList();
    }

    private void dealWithOperationList() {
        if (mRemoteOperationList != null && mRemoteOperationList.size() > 0) {
            Log.d(TAG, "final apply batch mRemoteOperationList.size(): "
                    + mRemoteOperationList.size() + " mCount: "+mCount);
            pushIntoContentResolver(mRemoteOperationList);
            mRemoteOperationList = null;
        }
    }

    private void queryAndSyncGroup() {
        mTitleGroupAccountsKey.clear();
        Set<String> needSyncTitles = new HashSet<>();
        Uri groupUri = MzConstants.Dialer.MzGroup.VIEW_GROUPS;
        Cursor cursor = null;
        try {
            cursor = mResolver.query(groupUri, MzGroupMetaDataLoad.COLUMNS, null,
                    null, null);
            if (cursor != null && !cursor.isClosed()) {
                cursor.moveToPosition(-1);
                while ((cursor.moveToNext())) {
                    String title = cursor.getString(MzGroupMetaDataLoad.TITLE);
                    String accountName = cursor.getString(MzGroupMetaDataLoad.ACCOUNT_NAME);
                    String accountType = cursor.getString(MzGroupMetaDataLoad.ACCOUNT_TYPE);
                    String dataSet = cursor.getString(MzGroupMetaDataLoad.DATA_SET);
                    AccountWithDataSet accountWithDataSet = new AccountWithDataSet(accountName, accountType, dataSet);
                    long groupId = cursor.getLong(MzGroupMetaDataLoad.GROUP_ID);
                    if (TextUtils.isEmpty(title)) {
                        continue;
                    }
                    title = title.trim();
                    if (TextUtils.isEmpty(accountName) && TextUtils.isEmpty(accountType)) {
                        accountName = MzContactsContract.MzAccounts.DEVICES_ONLY_ACCOUNT.name;
                        accountType = MzContactsContract.MzAccounts.DEVICES_ONLY_ACCOUNT.type;
                    }

                    GroupAccountInfo groupAccountInfo = new GroupAccountInfo(accountName, accountType, dataSet,
                            groupId, title, 0, 0);

                    if(mSaveSyncAccounts.contains(accountWithDataSet)) {
                        needSyncTitles.add(title);
                    }

                    ArrayList<GroupAccountInfo> groupAccountInfos = mTitleGroupAccountsKey.get(title);
                    if (groupAccountInfos == null) {
                        ArrayList<GroupAccountInfo> tempList = new ArrayList<>();
                        tempList.add(groupAccountInfo);
                        mTitleGroupAccountsKey.put(title, tempList);
                    } else {
                        groupAccountInfos.add(groupAccountInfo);
                    }
                }
            }
        } catch (Exception e) {
            Log.d(TAG, "query group found exception");
        } finally {
            MoreCloseables.closeQuietly(cursor);
        }

        if (needSyncTitles.size() != 0) {
            startSyncGroups(needSyncTitles);
        }
    }

    private void startSyncGroups(Set<String> syncTitleSet) {
        mInsertTitleAccountKey.clear();

        for (String title : syncTitleSet) {
            ArrayList<GroupAccountInfo> gAccountInfors = mTitleGroupAccountsKey.get(title);
            Set<AccountWithDataSet> titleAccountSet = new HashSet<>();
            if (gAccountInfors != null) {
                for (GroupAccountInfo groupAccountInfo : gAccountInfors) {
                    String aN = groupAccountInfo.getAccountName();
                    String aT = groupAccountInfo.getAccountType();
                    String dS = groupAccountInfo.getDataSet();
                    AccountWithDataSet account = new AccountWithDataSet(aN, aT, dS);
                    titleAccountSet.add(account);
                }
                for (AccountWithDataSet syncAccount : mSaveSyncAccounts) {
                    if (!titleAccountSet.contains(syncAccount)) {
                        ContentValues values = new ContentValues();
                        values.put(ContactsContract.Groups.ACCOUNT_TYPE, syncAccount.type);
                        values.put(ContactsContract.Groups.ACCOUNT_NAME, syncAccount.name);
                        values.put(ContactsContract.Groups.DATA_SET, syncAccount.dataSet);
                        values.put(ContactsContract.Groups.TITLE, title);

                        final Uri groupUri = mResolver.insert(ContactsContract.Groups.CONTENT_URI,
                                values);
                        if (groupUri == null) {
                            Log.d(TAG, "group insert has problem : groupUri is null");
                            continue;
                        }
                        long insertGroupId = ContentUris.parseId(groupUri);
                        HashMap<AccountWithDataSet, Long> insertAccountGroup = new HashMap<>();
                        insertAccountGroup.put(syncAccount, insertGroupId);
                        mInsertTitleAccountKey.put(title, insertAccountGroup);
                    }
                }
            }
        }
    }

    private void averageContactCount(ArrayList<Long> contactIds, boolean hasPhotoContacts) {
        int total = contactIds.size();
        int count = total / MAX_CURSOR_NUMBER;
        int extraCount = total % MAX_CURSOR_NUMBER;
        ArrayList<Long> checkedIdList = new ArrayList<>();

        if (count > 0) {
            for (int i = 0; i < count; i++) {
                checkedIdList.clear();
                for (int j = i * MAX_CURSOR_NUMBER; j < (i + 1) * MAX_CURSOR_NUMBER; j++) {
                    checkedIdList.add(contactIds.get(j));
                }
                queryAndSynContacts(checkedIdList, hasPhotoContacts);
            }
        }

        checkedIdList.clear();

        for (int i = count * MAX_CURSOR_NUMBER; i < count * MAX_CURSOR_NUMBER + extraCount; i++) {
            checkedIdList.add(contactIds.get(i));
        }
        if (checkedIdList.size() > 0) {
            queryAndSynContacts(checkedIdList, hasPhotoContacts);
        }
    }

    private void queryAndSynContacts(ArrayList<Long> contactIds, boolean isOnlyHasPhotoContact) {

        Uri dataUri = MzDialerProviderContract.ViewData.VIEW_DATA_URI;
        Cursor cursor = null;
        long contactId = 0l;
        long rawContactId;
        long currentContactId = 0l;
        boolean hasData = false;
        Uri uri = null;
        String photoUri;
        String lookupKey = null;
        Set<Long> contactIdSet = new HashSet<>();
        LongSparseArray<AccountWithDataSet> rawContactAccountMap = new LongSparseArray<>();
        Set<AccountWithDataSet> contactAccounts = new HashSet<>();
        LongSparseArray<Set<Integer>> rawContactDeletedKey = new LongSparseArray<>();
        StringBuilder selection = new StringBuilder();
        ArrayList<String> selectionArgs = new ArrayList<>();
        int contactCount = contactIds.size();
        selection.append(MzData.CONTACT_ID + " IN(");
        int index = 0;
        for (Long id : contactIds) {
            if (index > 0) {
                selection.append(',');
            }
            if (contactCount > GroupUtils.MAX_NUMBER_DATA_DEFAULT) {
                selection.append(String.valueOf(id));
            } else {
                selection.append('?');
                selectionArgs.add(String.valueOf(id));
            }
            index++;
        }
        selection.append(')');

        String accountName;
        String accountType;
        String dataSet;
        try {
            cursor = mResolver.query(dataUri, DataQuery.DATA_COLUMNS, selection.toString(),
                    selectionArgs.toArray(new String[0]), MzData.CONTACT_ID);

            if (cursor != null) {
                cursor.moveToPosition(-1);
                while (cursor.moveToNext()) {
                    contactId = cursor.getLong(DataQuery.CONTACT_ID);
                    photoUri = cursor.getString(DataQuery.PHOTO_URI);
                    long modifyTime = cursor.getLong(DataQuery.MODIFY_TIME);
                    int deleted = cursor.getInt(DataQuery.DELETED);
                    long dataId = cursor.getLong(DataQuery.DATA_ID);
                    String mimetype = cursor.getString(DataQuery.MIMETYPE);
                    accountName = cursor.getString(DataQuery.ACCOUNT_NAME);
                    accountType = cursor.getString(DataQuery.ACCOUNT_TYPE);
                    dataSet = cursor.getString(DataQuery.DATA_SET);
                    rawContactId = cursor.getLong(DataQuery.RAW_CONTACT_ID);
                    String modifyKey = cursor.getString(DataQuery.MODIFY_KEY);
                    String syncKey = cursor.getString(DataQuery.SYNC_KEY);
                    boolean isPrimary = cursor.getInt(DataQuery.IS_PRIMARY) == 0;
                    boolean isSuperPrimary = cursor.getInt(DataQuery.IS_SUPERPRIMARY) == 0;

                    if (currentContactId != contactId && hasData) {
                        uri = ContactsContract.Contacts.getLookupUri(currentContactId, lookupKey);
                        buildSyncOperation(currentContactId, uri, contactAccounts, rawContactAccountMap, rawContactDeletedKey);
                        contactAccounts.clear();
                        rawContactAccountMap.clear();
                        rawContactDeletedKey.clear();
                    }

                    AccountWithDataSet account = new AccountWithDataSet(accountName, accountType, dataSet);
                    contactAccounts.add(account);
                    rawContactAccountMap.put(rawContactId, account);

                    if (!GroupMembership.CONTENT_ITEM_TYPE.equals(mimetype)
                            && !SipAddress.CONTENT_ITEM_TYPE.equals(mimetype)) {
                        Set<Integer> deleteSet = rawContactDeletedKey.get(rawContactId);
                        if (deleteSet == null) {
                            Set<Integer> tempSet = new HashSet<>();
                            tempSet.add(deleted);
                            rawContactDeletedKey.put(rawContactId, tempSet);
                        } else {
                            deleteSet.add(deleted);
                        }
                    }

                    if (!contactIdSet.contains(contactId)) {
                        lookupKey = cursor.getString(DataQuery.DATA_LOOK_UP_KEY);
                        mCustomRingTone = cursor.getString(DataQuery.CUSTOM_RINGTONE);
                        currentContactId = contactId;
                        contactIdSet.add(contactId);
                        uri = ContactsContract.Contacts.getLookupUri(contactId, lookupKey);
                    }

                    if (StructuredName.CONTENT_ITEM_TYPE.equals(mimetype)) {
                        String displayName = cursor.getString(DataQuery.DATA1);
                        String giveName = cursor.getString(DataQuery.DATA2);
                        String familyName = cursor.getString(DataQuery.DATA3);
                        String prefix = cursor.getString(DataQuery.DATA4);
                        String middle = cursor.getString(DataQuery.DATA5);
                        String suffer = cursor.getString(DataQuery.DATA6);
                        String phoneTicGivenName = cursor.getString(DataQuery.DATA7);
                        String phoneTicMiddleName = cursor.getString(DataQuery.DATA8);
                        String phoneTicFamilyName = cursor.getString(DataQuery.DATA9);

                        NameData nameData = new NameData(dataId, rawContactId, displayName, giveName,
                                familyName, prefix, middle, suffer, phoneTicGivenName,
                                phoneTicMiddleName, phoneTicFamilyName, modifyTime, deleted, modifyKey, syncKey);
                        mNameList.add(nameData);

                    } else if (Phone.CONTENT_ITEM_TYPE.equals(mimetype)) {
                        String number = cursor.getString(DataQuery.DATA1);
                        int type = cursor.getInt(DataQuery.DATA2);
                        String label = cursor.getString(DataQuery.DATA3);

                        PhoneData phoneData = new PhoneData(dataId, rawContactId, number, type, label,
                                isPrimary, isSuperPrimary, modifyTime, deleted, modifyKey, syncKey);
                        mPhoneList.add(phoneData);

                    } else if (Organization.CONTENT_ITEM_TYPE.equals(mimetype)) {
                        String company = cursor.getString(DataQuery.DATA1);
                        String department = cursor.getString(DataQuery.DATA5);
                        String title = cursor.getString(DataQuery.DATA4);
                        String phoneName = cursor.getString(DataQuery.DATA8);

                        OrganizationData organizationData = new OrganizationData(dataId, rawContactId,
                                company, department, title, phoneName, isPrimary, modifyTime, deleted,
                                modifyKey, syncKey);
                        mOrganizationList.add(organizationData);
                    } else if (StructuredPostal.CONTENT_ITEM_TYPE.equals(mimetype)) {
                        String address = cursor.getString(DataQuery.DATA1);
                        String street = cursor.getString(DataQuery.DATA4);
                        String pobox = cursor.getString(DataQuery.DATA5);
                        String localty = cursor.getString(DataQuery.DATA7);
                        String region = cursor.getString(DataQuery.DATA8);
                        String postCode = cursor.getString(DataQuery.DATA9);
                        String country = cursor.getString(DataQuery.DATA10);
                        int type = cursor.getInt(DataQuery.DATA2);
                        String label = cursor.getString(DataQuery.DATA3);

                        PostalData postalData = new PostalData(dataId, rawContactId, address, street,
                                pobox, localty, region, postCode, country, label, type, isPrimary,
                                isSuperPrimary, modifyTime, deleted, modifyKey, syncKey);
                        mPostalList.add(postalData);

                    } else if (Email.CONTENT_ITEM_TYPE.equals(mimetype)) {
                        String emailString = cursor.getString(DataQuery.DATA1);
                        int type = cursor.getInt(DataQuery.DATA2);
                        String label = cursor.getString(DataQuery.DATA3);
                        EmailData emailData = new EmailData(dataId, rawContactId, emailString, type,
                                label, isPrimary, isSuperPrimary, modifyTime, deleted, modifyKey, syncKey);
                        mEmailList.add(emailData);

                    } else if (Photo.CONTENT_ITEM_TYPE.equals(mimetype)) {
                        byte[] photoBytes = cursor.getBlob(DataQuery.DATA15);
                        PhotoData photoData = new PhotoData(dataId, rawContactId, photoUri, photoBytes, isPrimary);
                        mPhotoList.add(photoData);

                    } else if (Event.CONTENT_ITEM_TYPE.equals(mimetype)) {
                        String birth = cursor.getString(DataQuery.DATA1);
                        int type = cursor.getInt(DataQuery.DATA2);
                        String label = cursor.getString(DataQuery.DATA3);
                        EventData eventData = new EventData(dataId, rawContactId, birth, type, label,
                                modifyTime, deleted, modifyKey, syncKey);
                        mEventList.add(eventData);

//                    } else if (GroupMembership.CONTENT_ITEM_TYPE.equals(mimetype)) {
//                        long groupId = cursor.getLong(DataQuery.DATA1);
//                        GroupData groupData = new GroupData(null, dataId, rawContactId, groupId,
//                                accountName, accountType, dataSet);
//                        mGroupList.add(groupData);

                    } else if (Im.CONTENT_ITEM_TYPE.equals(mimetype)) {
                        int protocol = cursor.getInt(DataQuery.DATA5);
                        String customProtocol = cursor.getString(DataQuery.DATA6);
                        String address = cursor.getString(DataQuery.DATA1);
                        int type = cursor.getInt(DataQuery.DATA2);

                        ImData imData = new ImData(dataId, rawContactId, protocol, customProtocol,
                                address, type, isPrimary, modifyTime, deleted, modifyKey, syncKey);
                        mImList.add(imData);

                    } else if (Note.CONTENT_ITEM_TYPE.equals(mimetype)) {
                        String noteString = cursor.getString(DataQuery.DATA1);
                        NoteData noteData = new NoteData(dataId, rawContactId, noteString, modifyTime,
                                deleted, modifyKey, syncKey);
                        mNoteList.add(noteData);

                    } else if (Website.CONTENT_ITEM_TYPE.equals(mimetype)) {
                        String webString = cursor.getString(DataQuery.DATA1);
                        int type = cursor.getInt(DataQuery.DATA2);
                        WebsiteData websiteData = new WebsiteData(dataId, rawContactId, webString, type,
                                modifyTime, deleted, modifyKey, syncKey);
                        mWebsiteList.add(websiteData);

                    } else {
                        //todo custom mimeType
                    }
                    hasData = true;
                }
                if (hasData) {
                    buildSyncOperation(contactId, uri, contactAccounts, rawContactAccountMap, rawContactDeletedKey);
                }
            }
        } catch (Exception e) {
            if (mService != null) {
                mService.changeSyncState(false, false);
            }
            Log.e(TAG, String.format("%s: %s", e.toString(), e.getMessage()));
        } finally {
            MoreCloseables.closeQuietly(cursor);
        }
    }

    private void buildSyncOperation(long contactId, Uri contactUri, Set contactAccounts,
                                    LongSparseArray<AccountWithDataSet> rawContactAccountMap,
                                    LongSparseArray<Set<Integer>> rawContactIdDetetedMap) {

        mRemoteOperationList = constructSyncOperations(mRemoteOperationList, contactAccounts,
                rawContactAccountMap, rawContactIdDetetedMap);
        if (mRemoteOperationList == null || mRemoteOperationList.size() == 0) {
            Log.d(TAG, "mRemoteOperationList == null or mRemoteOperationList.size() == 0 : " + contactId);
            return;
        }
        mCount++;

        if (mRemoteOperationList.size() > MAX_APPLY_BATCH) {
            Log.d(TAG, "apply batch: " + mRemoteOperationList.size());
            pushIntoContentResolver(mRemoteOperationList);
            mRemoteOperationList = null;
        }
    }

    public ArrayList<ContentProviderOperation> constructSyncOperations(
            ArrayList<ContentProviderOperation> remoteOperationList,
            Set contactAccounts, LongSparseArray<AccountWithDataSet> rawContactAccountMap,
            LongSparseArray<Set<Integer>> rawContactIdDetetedMap) {
        if (remoteOperationList == null) {
            remoteOperationList = new ArrayList<>();
        }
        //如果当前的rawContact对应的data都是delete状态，则删除该rawContactId
        // ，且不需要把该rawContact下的数据插入到需要同步的帐号
        Set<Long> deleteRawContactId = new HashSet<>();
        for (int i = 0; i < rawContactIdDetetedMap.size(); i++) {
            long rawContactId = rawContactIdDetetedMap.keyAt(i);
            Set<Integer> deleteSet = rawContactIdDetetedMap.get(rawContactId);
            if (deleteSet.size() == 1 && deleteSet.contains(1)) {
                ContentProviderOperation deleteOperation = ContentProviderOperation
                        .newDelete(ContactsContract.RawContacts.CONTENT_URI)
                        .withSelection(ContactsContract.RawContacts._ID + " = ?",
                                new String[]{String.valueOf(rawContactId)})
                        .withYieldAllowed(true)
                        .build();
                remoteOperationList.add(deleteOperation);
                deleteRawContactId.add(rawContactId);
                Log.d(TAG, "deleteRawContactId +" + rawContactId);
            }
        }

        if (deleteRawContactId.size() == rawContactAccountMap.size()) {
            Log.d(TAG, "delete rawContactId--no need sync to other accounts");
            return remoteOperationList;
        }

        if (mNameList.size() == 0 && mPhoneList.size() == 0 && mEmailList.size() == 0) {
            Log.d(TAG, "name,email and phone is null--no need sync");
            return remoteOperationList;
        }

        HashMap<AccountWithDataSet, Integer> insertAccountBackIndex = new HashMap<>();
        //need insert
        for (AccountWithDataSet account : mSaveSyncAccounts) {
            if (account != null && !contactAccounts.contains(account)) {
                final int backReferenceIndex = remoteOperationList.size();
                ContentProviderOperation.Builder builder = ContentProviderOperation
                        .newInsert(ContactsContract.RawContacts.CONTENT_URI);

                builder.withValue(ContactsContract.RawContacts.ACCOUNT_NAME, account.name);
                builder.withValue(ContactsContract.RawContacts.ACCOUNT_TYPE, account.type);

                if (!TextUtils.isEmpty(mCustomRingTone)) {
                    builder.withValue(ContactsContract.RawContacts.CUSTOM_RINGTONE, mCustomRingTone);
                }
                remoteOperationList.add(builder.build());
                insertAccountBackIndex.put(account, backReferenceIndex);
            }
        }

        MzSyncDataEntryIterator dataEntryIterator = new SyncOperationConstrutor();
        iterateAllData(dataEntryIterator, insertAccountBackIndex, rawContactAccountMap);
        dataEntryIterator.build(remoteOperationList);

        return remoteOperationList;
    }

    private void iterateAllData(MzSyncDataEntryIterator dataEntryIterator,
                                HashMap<AccountWithDataSet, Integer> insertAccountBackIndex,
                                LongSparseArray<AccountWithDataSet> rawContactAccountMap) {
        if (mNameList.size() > 0) {
            iterateSingleDataList(mNameList, dataEntryIterator, insertAccountBackIndex, rawContactAccountMap);
        }

        if (mPhoneList.size() > 0) {
            iterateDataEntryList(mPhoneList, dataEntryIterator, insertAccountBackIndex, rawContactAccountMap);
        }

        if (mOrganizationList.size() > 0) {
            iterateSingleDataList(mOrganizationList, dataEntryIterator, insertAccountBackIndex, rawContactAccountMap);
        }

        if (mPostalList.size() > 0) {
            iterateDataEntryList(mPostalList, dataEntryIterator, insertAccountBackIndex, rawContactAccountMap);
        }

        if (mEmailList.size() > 0) {
            iterateDataEntryList(mEmailList, dataEntryIterator, insertAccountBackIndex, rawContactAccountMap);
        }

        if (mPhotoList.size() > 0) {
            iteratePhotoList(mPhotoList, dataEntryIterator, insertAccountBackIndex, rawContactAccountMap);
        }

        if (mEventList.size() > 0) {
            iterateDataEntryList(mEventList, dataEntryIterator, insertAccountBackIndex, rawContactAccountMap);
        }

        if (mImList.size() > 0) {
            iterateDataEntryList(mImList, dataEntryIterator, insertAccountBackIndex, rawContactAccountMap);
        }

        if (mNoteList.size() > 0) {
            iterateSingleDataList(mNoteList, dataEntryIterator, insertAccountBackIndex, rawContactAccountMap);
        }

        if (mWebsiteList.size() > 0) {
            iterateDataEntryList(mWebsiteList, dataEntryIterator, insertAccountBackIndex, rawContactAccountMap);
        }

//        if (mGroupList.size() > 0) {
//            iterateGroupEntryList(mGroupList, dataEntryIterator, insertAccountBackIndex, rawContactAccountMap);
//        }

        mNameList.clear();
        mPhoneList.clear();
        mOrganizationList.clear();
        mPostalList.clear();
        mEmailList.clear();
        mPhotoList.clear();
        mEventList.clear();
        mImList.clear();
        mNoteList.clear();
        mWebsiteList.clear();

//        mGroupList.clear();
    }

    private void iterateDataEntryList(ArrayList<? extends MzSyncDataEntry> dataList,
                                      MzSyncDataEntryIterator dataEntryIterator,
                                      HashMap<AccountWithDataSet, Integer> insertAccountBackIndex,
                                      LongSparseArray<AccountWithDataSet> rawContactAccountMap) {
        LongSparseArray<ArrayList<MzSyncDataEntry>> rawContactDataKey = new LongSparseArray<>();
        LongSparseArray<Set<String>> rawContactData1Key = new LongSparseArray<>();
        ArrayList<MzSyncDataEntry> needInsertData = new ArrayList<>();
        ArrayList<MzSyncDataEntry> needDeleteData = new ArrayList<>();
        ArrayList<MzSyncDataEntry> needUpdateData = new ArrayList<>();
        HashSet<String> dataSet = new HashSet<>();
        Set<Long> rawContactIdSet = new HashSet<>();
        for (MzSyncDataEntry syncData : dataList) {
            String data1 = syncData.getData1();
            long rawContactId = syncData.getRawContactId();
            String modifyKey = syncData.getModifyKey();
            String syncKey = syncData.getSyncData();
            rawContactIdSet.add(rawContactId);
            ArrayList<MzSyncDataEntry> rawDataArrayList = rawContactDataKey.get(rawContactId);
            if (rawDataArrayList == null) {
                ArrayList tempList = new ArrayList();
                tempList.add(syncData);
                rawContactDataKey.put(rawContactId, tempList);
            } else {
                rawDataArrayList.add(syncData);
            }
            Set<String> set = rawContactData1Key.get(rawContactId);
            if (set == null) {
                Set<String> st = new HashSet<>();
                st.add(data1);
                rawContactData1Key.put(rawContactId, st);
            } else {
                set.add(data1);
            }

            if (syncData.isDeleted()) {
                needDeleteData.add(syncData);
            } else {
                if (syncData.getModifyTime() > 0 && !TextUtils.equals(modifyKey, syncKey)) {
                    needUpdateData.add(syncData);
                } else {
                    if (!dataSet.contains(data1)) {
                        dataSet.add(data1);
                        needInsertData.add(syncData);
                    }
                }
            }
        }

        if (needDeleteData.size() > 0) {
            for (MzSyncDataEntry dataEntry : needDeleteData) {
                dataEntryIterator.onDeleteDataEntry(dataEntry);
            }
        }

        if (insertAccountBackIndex.size() > 0 && needInsertData.size() > 0) {
            Iterator it = insertAccountBackIndex.keySet().iterator();
            while (it.hasNext()) {
                AccountWithDataSet accountWithDataSet = (AccountWithDataSet) it.next();
                int backReferenceIndex = insertAccountBackIndex.get(accountWithDataSet);
                for (MzSyncDataEntry insertInsert : needInsertData) {
                    dataEntryIterator.onInsertDataEntry(insertInsert, backReferenceIndex);
                }
            }
        }

        if (rawContactData1Key.size() > 0) {
            if (rawContactData1Key.size() == rawContactAccountMap.size()) {
                for (int i = 0; i < rawContactData1Key.size(); i++) {
                    long rawContactId = rawContactData1Key.keyAt(i);
                    Set<String> rawData1Set = rawContactData1Key.get(rawContactId);
                    if (rawData1Set != null) {
                        for (MzSyncDataEntry diffInsertData : needInsertData) {
                            String data1 = diffInsertData.getData1();
                            if (!rawData1Set.contains(data1)) {
                                dataEntryIterator.onInsertDifferentEntry(diffInsertData, rawContactId);
                            }
                        }
                    }
                }
            } else {
                for (int i = 0; i < rawContactAccountMap.size(); i++) {
                    long rawContactId = rawContactAccountMap.keyAt(i);
                    if (!rawContactIdSet.contains(rawContactId)) {
                        for (MzSyncDataEntry diffInsertData : needInsertData) {
                            dataEntryIterator.onInsertDifferentEntry(diffInsertData, rawContactId);
                        }
                    }
                }
            }
        }


        if (needUpdateData.size() > 0) {
            for (MzSyncDataEntry modifyData : needUpdateData) {
                for (MzSyncDataEntry mainSyncData : dataList) {
                    String data1 = mainSyncData.getData1();
                    if (TextUtils.equals(modifyData.getModifyKey(), data1)) {
                        dataEntryIterator.onUpdateDataEntry(modifyData, mainSyncData);
                    }
                }
            }
        }
    }

    private void iterateSingleDataList(ArrayList<? extends MzSyncDataEntry> dataList,
                                       MzSyncDataEntryIterator dataEntryIterator,
                                       HashMap<AccountWithDataSet, Integer> insertAccountBackIndex,
                                       LongSparseArray<AccountWithDataSet>  rawContactAccountMap) {

        Set<Long> rawContactIdSet = new HashSet<>();
        ArrayList<MzSyncDataEntry> needUpdateData = new ArrayList<>();
        ArrayList<MzSyncDataEntry> needDeleteData = new ArrayList<>();
        MzSyncDataEntry insertData = null;
        for (MzSyncDataEntry dataEntry : dataList) {
            String data1 = dataEntry.getData1();
            String modifyKey = dataEntry.getModifyKey();
            String syncKey = dataEntry.getSyncData();

            long rawContactId = dataEntry.getRawContactId();
            rawContactIdSet.add(rawContactId);
            if (dataEntry.isDeleted()) {
                needDeleteData.add(dataEntry);
            } else {
                if (dataEntry.getModifyTime() > 0 && !TextUtils.equals(modifyKey, syncKey)) {
                    needUpdateData.add(insertData);
                } else {
                    if (!TextUtils.isEmpty(data1)) {
                        insertData = dataEntry;
                    }
                }
            }
        }

        if (needDeleteData.size() > 0) {
            for (MzSyncDataEntry deleteData : needDeleteData) {
                dataEntryIterator.onDeleteDataEntry(deleteData);
            }
        }


        if (insertAccountBackIndex.size() > 0 && insertData != null) {
            for (Object accountWithDataSet : insertAccountBackIndex.keySet()) {
                int backReferenceIndex = insertAccountBackIndex.get(accountWithDataSet);
                dataEntryIterator.onInsertDataEntry(insertData, backReferenceIndex);
            }
        }

        if (rawContactIdSet.size() != rawContactAccountMap.size() && insertData != null) {
            for (int i = 0; i < rawContactAccountMap.size(); i++) {
                long rawContactId = rawContactAccountMap.keyAt(i);
                if (!rawContactIdSet.contains(rawContactId)) {
                    dataEntryIterator.onInsertDifferentEntry(insertData, rawContactId);
                }
            }
        }

        //4、更新字段
        if (needUpdateData.size() > 0) {
            for (MzSyncDataEntry modifyData : needUpdateData) {
                for (MzSyncDataEntry mainImData : dataList) {
                    String string = mainImData.getData1();
                    if (TextUtils.equals(modifyData.getModifyKey(), string)) {
                        dataEntryIterator.onUpdateDataEntry(modifyData, mainImData);
                    }
                }
            }
        }
    }

    private void iteratePhotoList(ArrayList<PhotoData> photoDatas,
                                  MzSyncDataEntryIterator dataEntryIterator,
                                  HashMap<AccountWithDataSet, Integer> insertAccountBackIndex,
                                  LongSparseArray<AccountWithDataSet> rawContactAccountMap) {
        Set<Long> photoSet = new HashSet<>();
        PhotoData insertPhotoData = null;
        for (PhotoData photoData : photoDatas) {
            byte[] bytes = photoData.mBytes;
            if (bytes != null && bytes.length > 0) {
                insertPhotoData = photoData;
            }
            long rawContactId = photoData.mRawContactId;
            photoSet.add(rawContactId);
        }

        if (insertAccountBackIndex.size() > 0 && insertPhotoData != null) {
            Iterator it = insertAccountBackIndex.keySet().iterator();
            while (it.hasNext()) {
                AccountWithDataSet accountWithDataSet = (AccountWithDataSet) it.next();
                int backReferenceIndex = insertAccountBackIndex.get(accountWithDataSet);
                dataEntryIterator.onInsertDataEntry(insertPhotoData, backReferenceIndex);
            }
        }

        if (photoSet.size() != rawContactAccountMap.size() && insertPhotoData != null) {
            for (int i = 0; i < rawContactAccountMap.size(); i++) {
                long rawContactId = rawContactAccountMap.keyAt(i);
                if (!photoSet.contains(rawContactId)) {
                    dataEntryIterator.onInsertDifferentEntry(insertPhotoData, rawContactId);
                }
            }
        }
    }

    private void iterateGroupEntryList(ArrayList<GroupData> groupDatas,
                                       MzSyncDataEntryIterator dataEntryIterator,
                                       HashMap<AccountWithDataSet, Integer> insertAccountBackIndex,
                                       LongSparseArray<AccountWithDataSet> rawContactAccountMap) {
        if (mTitleGroupAccountsKey.size() == 0) {
            Log.d(TAG, "no groups in accounts");
            return;
        }
        LongSparseArray<Set<Long>> rawContactIdGroupIdKey = new LongSparseArray();
        Set<Long> groupIdSet = new HashSet<>();
        Set<String> allTitleSet = new HashSet<>();
        LongSparseArray<String> groupIdTitle = new LongSparseArray<>();
        HashMap<String, ArrayList<GroupAccountInfo>> titleGroupAccountInforsKey = new HashMap<>();
        Set<Long> rawContactIdSet = new HashSet<>();
        for (GroupData groupData : groupDatas) {
            long rawContactId = groupData.mRawContactId;
            long groupId = groupData.mGroupId;
            groupIdSet.add(groupId);
            rawContactIdSet.add(rawContactId);
            Set<Long> groupIdKey = rawContactIdGroupIdKey.get(rawContactId);
            if (groupIdKey == null) {
                Set<Long> set = new HashSet<>();
                set.add(groupId);
                rawContactIdGroupIdKey.put(rawContactId, set);
            } else {
                groupIdKey.add(groupId);
            }
        }

        Iterator it = mTitleGroupAccountsKey.keySet().iterator();
        while (it.hasNext()) {
            String title = (String) it.next();
            ArrayList<GroupAccountInfo> groupAccountInfos = mTitleGroupAccountsKey.get(title);
            for (GroupAccountInfo groupAccountInfo : groupAccountInfos) {
                long groupId = groupAccountInfo.getGroupId();
                groupIdTitle.put(groupId, title);
                if (groupIdSet.contains(groupId)) {
                    allTitleSet.add(title);
                }
            }
        }

        if (insertAccountBackIndex.size() > 0 && allTitleSet.size() > 0) {
            Iterator iterator = insertAccountBackIndex.keySet().iterator();
            while (iterator.hasNext()) {
                AccountWithDataSet accountWithDataSet = (AccountWithDataSet) iterator.next();
                int backReferenceIndex = insertAccountBackIndex.get(accountWithDataSet);
                for (String title : allTitleSet) {
                    GroupData groupData = conversionTitleInAccountGroupData(title, accountWithDataSet);
                    dataEntryIterator.onInsertDataEntry(groupData, backReferenceIndex);
                }
            }
        }
    }

    private GroupData conversionTitleInAccountGroupData(String title, AccountWithDataSet account) {
        long insertGroupId = -1;
        boolean hasGroupInAccount = false;
        GroupData groupData;
        ArrayList<GroupAccountInfo> groupAccountInfos = mTitleGroupAccountsKey.get(title);
        if (groupAccountInfos != null) {
            for (GroupAccountInfo groupAccountInfo : groupAccountInfos) {
                long groupId = groupAccountInfo.getGroupId();
                String accountName = groupAccountInfo.getAccountName();
                String accoutType = groupAccountInfo.getAccountType();
                String dataSet = groupAccountInfo.getDataSet();

                AccountWithDataSet at = new AccountWithDataSet(accountName, accoutType, dataSet);
                if (at.equals(account)) {
                    hasGroupInAccount = true;
                    insertGroupId = groupId;
                    break;
                }
            }
        }

        if(!hasGroupInAccount) {
            HashMap<AccountWithDataSet, Long> inertAccountGroupIdKey = mInsertTitleAccountKey.get(title);
            if(inertAccountGroupIdKey !=null) {
                Long groupId = inertAccountGroupIdKey.get(account);
                insertGroupId = (groupId == null ? -1 : groupId);
            }
        }

        groupData = new GroupData(title, 0, 0, insertGroupId, account.name,
                account.type, account.dataSet);
        return groupData;
    }

    private class SyncOperationConstrutor implements MzSyncDataEntryIterator {
        private final List<ContentProviderOperation.Builder> mOperationBuilderList;

        public SyncOperationConstrutor() {
            mOperationBuilderList = new ArrayList<>();
        }

        public void onDeleteDataEntry(MzSyncDataEntry elem) {
            if (elem != null && !elem.isEmpty()) {
                addOperation(elem.constructDeleteOperation());
            }
        }

        public void onUpdateDataEntry(MzSyncDataEntry elem, MzSyncDataEntry mainEntry) {
            if (elem != null && mainEntry != null && !elem.isEmpty()) {
                addOperation(elem.constructUpdateOpertation(mainEntry));
            }
        }

        @Override
        public void onInsertDataEntry(MzSyncDataEntry entry, int backReferenceIndex) {
            if (entry != null && !entry.isEmpty()) {
                addOperation(entry.constructInsertOperation(backReferenceIndex));
            }
        }

        @Override
        public void onInsertDifferentEntry(MzSyncDataEntry entry, long rawContactId) {
            if (entry != null && !entry.isEmpty()) {
                addOperation(entry.constructDifferentInsertOperation(rawContactId));
            }
        }

        private void addOperation(ContentProviderOperation.Builder builder) {
            if (builder != null) {
                mOperationBuilderList.add(builder);
            }
        }

        @Override
        public void build(List<ContentProviderOperation> operations) {
            if (mOperationBuilderList.size() <= 0) {
                return;
            }

            mOperationBuilderList.get(mOperationBuilderList.size() - 1).withYieldAllowed(true);
            for (ContentProviderOperation.Builder builder : mOperationBuilderList) {
                operations.add(builder.build());
            }
        }
    }

    private void pushIntoContentResolver(ArrayList<ContentProviderOperation> operationList) {
        ArrayList<Uri> resultUris = new ArrayList<>();
        if (mResolver == null) {
            return;
        }

        try {
            final ContentProviderResult[] results = mResolver.applyBatch(
                    ContactsContract.AUTHORITY, operationList);

//            if (results.length == 0) {
//                return resultUris;
//            } else {
//                for (ContentProviderResult result : results) {
//                    if (result != null && result.uri != null
//                            && (result.uri).getEncodedPath().contains(
//                            RawContacts.CONTENT_URI.getEncodedPath())) {
//                        resultUris.add(result.uri);
//                    }
//                }
//            }
        } catch (RemoteException | OperationApplicationException e) {
            Log.e(TAG, String.format("%s: %s", e.toString(), e.getMessage()));
            if (mService != null) {
                mService.changeSyncState(false, false);
            }
        }
    }

    public class WebsiteData implements MzSyncDataEntry {

        private long mDataId;
        private long mRawContactId;
        private final String mWebsite;
        private final int mType;
        private long mModifyTime;
        private int mDeleted;
        private String mModifyKey;
        private String mSyncKey;


        public WebsiteData(long dataId, long rawContactId, String website, int type, long modifyTime, int deleted,
                           String modifyKey, String syncKey) {
            mDataId = dataId;
            mRawContactId = rawContactId;
            mWebsite = website;
            mType = type;
            mModifyTime = modifyTime;
            mDeleted = deleted;
            mModifyKey = modifyKey;
            mSyncKey = syncKey;
        }

        @Override
        public boolean isEmpty() {
            return TextUtils.isEmpty(mWebsite);
        }

        @Override
        public ContentProviderOperation.Builder constructDeleteOperation() {
            return ContentProviderOperation
                    .newDelete(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());
        }

        @Override
        public ContentProviderOperation.Builder constructUpdateOpertation(MzSyncDataEntry mainEntry) {
            WebsiteData websiteData = (WebsiteData) mainEntry;
            if (websiteData == null) {
                return null;
            }

            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newUpdate(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());

            builder.withValue(Website.URL, websiteData.mWebsite);
            builder.withValue(Website.TYPE, websiteData.mType);
            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructInsertOperation(int backReferenceIndex) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(Data.CONTENT_URI);
            builder.withValueBackReference(Website.RAW_CONTACT_ID, backReferenceIndex);
            builder.withValue(Data.MIMETYPE, Website.CONTENT_ITEM_TYPE);
            builder.withValue(Website.URL, mWebsite);
            builder.withValue(Website.TYPE, mType);
            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructDifferentInsertOperation(long rawContactId) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(Data.CONTENT_URI);
            builder.withValue(Website.RAW_CONTACT_ID, rawContactId);
            builder.withValue(Data.MIMETYPE, Website.CONTENT_ITEM_TYPE);
            builder.withValue(Website.URL, mWebsite);
            builder.withValue(Website.TYPE, mType);
            return builder;
        }

        @Override
        public String getSyncData() {
            if (!TextUtils.isEmpty(mSyncKey)) {
                return mSyncKey;
            } else {
                return mWebsite;
            }
        }

        @Override
        public String getData1() {
            return mWebsite;
        }

        @Override
        public long getRawContactId() {
            return mRawContactId;
        }

        @Override
        public String getModifyKey() {
            return mModifyKey;
        }

        @Override
        public boolean isDeleted() {
            return mDeleted == 1;
        }

        @Override
        public long getModifyTime() {
            return mModifyTime;
        }
    }

    public class NoteData implements MzSyncDataEntry {
        private long mDataId;
        private long mRawContactId;
        public final String mNote;
        private long mModifyTime;
        private int mDeleted;
        private String mModifyKey;
        private String mSyncKey;


        public NoteData(long dataId, long rawContactId, String note, long modifyTime, int deleted,
                        String modifyKey, String syncKey) {
            mDataId = dataId;
            mRawContactId = rawContactId;
            mNote = note;
            mModifyTime = modifyTime;
            mDeleted = deleted;
            mModifyKey = modifyKey;
            mSyncKey = syncKey;
        }


        @Override
        public boolean isEmpty() {
            return TextUtils.isEmpty(mNote);
        }

        @Override
        public ContentProviderOperation.Builder constructDeleteOperation() {
            return ContentProviderOperation
                    .newDelete(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());
        }

        @Override
        public ContentProviderOperation.Builder constructUpdateOpertation(MzSyncDataEntry mainEntry) {
            NoteData noteData = (NoteData) mainEntry;
            if (noteData == null) {
                return null;
            }
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newUpdate(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());

            builder.withValue(Note.NOTE, noteData.mNote);
            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructInsertOperation(int backReferenceIndex) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(Data.CONTENT_URI);
            builder.withValueBackReference(Note.RAW_CONTACT_ID, backReferenceIndex);
            builder.withValue(Data.MIMETYPE, Note.CONTENT_ITEM_TYPE);
            builder.withValue(Note.NOTE, mNote);

            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructDifferentInsertOperation(long rawContactId) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(Data.CONTENT_URI);
            builder.withValue(Note.RAW_CONTACT_ID, rawContactId);
            builder.withValue(Data.MIMETYPE, Note.CONTENT_ITEM_TYPE);
            builder.withValue(Note.NOTE, mNote);

            return builder;
        }

        @Override
        public String getSyncData() {
            if (!TextUtils.isEmpty(mSyncKey)) {
                return mSyncKey;
            } else {
                return mNote;
            }
        }

        @Override
        public String getData1() {
            return mNote;
        }

        @Override
        public long getRawContactId() {
            return mRawContactId;
        }

        @Override
        public String getModifyKey() {
            return mModifyKey;
        }

        @Override
        public boolean isDeleted() {
            return mDeleted == 1;
        }

        @Override
        public long getModifyTime() {
            return mModifyTime;
        }
    }

    public class ImData implements MzSyncDataEntry {
        private long mDataId;
        private long mRawContactId;
        private final String mAddress;
        private final int mProtocol;
        private final String mCustomProtocol;
        private final int mType;
        private final boolean mIsPrimary;
        private long mModifyTime;
        private int mDeleted;
        private String mModifyKey;
        private String mSyncKey;

        public ImData(long dataId, long rawContactId, final int protocol, final String customProtocol,
                      final String address, final int type, final boolean isPrimary, long modifyTime,
                      int deleted, String modifyKey, String syncKey) {
            mDataId = dataId;
            mRawContactId = rawContactId;
            mProtocol = protocol;
            mCustomProtocol = customProtocol;
            mType = type;
            mAddress = address;
            mIsPrimary = isPrimary;
            mModifyTime = modifyTime;
            mDeleted = deleted;
            mModifyKey = modifyKey;
            mSyncKey = syncKey;
        }

        @Override
        public boolean isEmpty() {
            return TextUtils.isEmpty(mAddress);
        }

        @Override
        public ContentProviderOperation.Builder constructDeleteOperation() {
            return ContentProviderOperation
                    .newDelete(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());
        }

        @Override
        public ContentProviderOperation.Builder constructInsertOperation(int backReferenceIndex) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(Data.CONTENT_URI);
            builder.withValueBackReference(Im.RAW_CONTACT_ID, backReferenceIndex);
            builder.withValue(Data.MIMETYPE, Im.CONTENT_ITEM_TYPE);
            builder.withValue(Im.TYPE, mType);
            builder.withValue(Im.PROTOCOL, mProtocol);
            builder.withValue(Im.DATA, mAddress);
            if (mProtocol == Im.PROTOCOL_CUSTOM) {
                builder.withValue(Im.CUSTOM_PROTOCOL, mCustomProtocol);
            }
            if (mIsPrimary) {
                builder.withValue(Data.IS_PRIMARY, 1);
            }
            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructUpdateOpertation(MzSyncDataEntry mainEntry) {
            ImData imData = (ImData) mainEntry;

            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newUpdate(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());
            builder.withValue(Im.TYPE, imData.mType);
            builder.withValue(Im.PROTOCOL, imData.mProtocol);
            builder.withValue(Im.DATA, imData.mAddress);
            if (imData.mProtocol == Im.PROTOCOL_CUSTOM) {
                builder.withValue(Im.CUSTOM_PROTOCOL, imData.mCustomProtocol);
            }
            if (imData.mIsPrimary) {
                builder.withValue(Data.IS_PRIMARY, 1);
            }
            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructDifferentInsertOperation(long rawContactId) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(Data.CONTENT_URI);
            builder.withValue(Im.RAW_CONTACT_ID, rawContactId);
            builder.withValue(Data.MIMETYPE, Im.CONTENT_ITEM_TYPE);
            builder.withValue(Im.TYPE, mType);
            builder.withValue(Im.PROTOCOL, mProtocol);
            builder.withValue(Im.DATA, mAddress);
            if (mProtocol == Im.PROTOCOL_CUSTOM) {
                builder.withValue(Im.CUSTOM_PROTOCOL, mCustomProtocol);
            }
            if (mIsPrimary) {
                builder.withValue(Data.IS_PRIMARY, 1);
            }

            return builder;
        }

        @Override
        public String getSyncData() {
            if (!TextUtils.isEmpty(mSyncKey)) {
                return mSyncKey;
            } else {
                return mAddress;
            }
        }

        @Override
        public String getData1() {
            return mAddress;
        }

        @Override
        public long getRawContactId() {
            return mRawContactId;
        }

        @Override
        public String getModifyKey() {
            return mModifyKey;
        }

        @Override
        public boolean isDeleted() {
            return mDeleted == 1;
        }

        @Override
        public long getModifyTime() {
            return mModifyTime;
        }
    }

    public class EventData implements MzSyncDataEntry {
        private final String mBirthday;
        private int mType;
        private String mLabel;
        private long mDataId;
        private long mRawContactId;
        private long mModifyTime;
        private int mDeleted;
        private String mModifyKey;
        private String mSyncKey;

        public EventData(long dataId, long rawContactId, String birthday, int type, String label,
                         long modifyTime, int deleted, String modifyKey, String syncKey) {
            mDataId = dataId;
            mRawContactId = rawContactId;
            mBirthday = birthday;
            mType = type;
            mLabel = label;
            mModifyTime = modifyTime;
            mDeleted = deleted;
            mModifyKey = modifyKey;
            mSyncKey = syncKey;
        }

        @Override
        public ContentProviderOperation.Builder constructDeleteOperation() {
            return ContentProviderOperation
                    .newDelete(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());
        }

        @Override
        public ContentProviderOperation.Builder constructInsertOperation(int backReferenceIndex) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation.newInsert(Data.CONTENT_URI);
            builder.withValueBackReference(Event.RAW_CONTACT_ID, backReferenceIndex);
            builder.withValue(Data.MIMETYPE, Event.CONTENT_ITEM_TYPE);
            builder.withValue(Event.START_DATE, mBirthday);

            builder.withValue(Event.TYPE, mType);
            if (mType == Event.TYPE_CUSTOM) {
                builder.withValue(Event.LABEL, mLabel);
            }
            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructUpdateOpertation(MzSyncDataEntry mainEntry) {
            EventData eventData = (EventData) mainEntry;

            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newUpdate(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());

            builder.withValue(Event.START_DATE, eventData.mBirthday);
            builder.withValue(Event.TYPE, eventData.mType);
            if (eventData.mType == Event.TYPE_CUSTOM) {
                builder.withValue(Event.LABEL, eventData.mLabel);
            }
            return builder;
        }


        @Override
        public ContentProviderOperation.Builder constructDifferentInsertOperation(long rawContactId) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation.newInsert(Data.CONTENT_URI);
            builder.withValue(Event.RAW_CONTACT_ID, rawContactId);
            builder.withValue(Data.MIMETYPE, Event.CONTENT_ITEM_TYPE);
            builder.withValue(Event.START_DATE, mBirthday);

            builder.withValue(Event.TYPE, mType);
            if (mType == Event.TYPE_CUSTOM) {
                builder.withValue(Event.LABEL, mLabel);
            }
            return builder;
        }

        @Override
        public String getSyncData() {
            if (!TextUtils.isEmpty(mSyncKey)) {
                return mSyncKey;
            } else {
                return mBirthday;
            }
        }

        @Override
        public String getData1() {
            return mBirthday;
        }

        @Override
        public long getRawContactId() {
            return mRawContactId;
        }

        @Override
        public String getModifyKey() {
            return mModifyKey;
        }

        @Override
        public boolean isDeleted() {
            return mDeleted == 1;
        }

        @Override
        public long getModifyTime() {
            return mModifyTime;
        }

        @Override
        public boolean isEmpty() {
            return TextUtils.isEmpty(mBirthday);
        }

    }

    public class NameData implements MzSyncDataEntry {
        private String mFamily;
        private String mGiven;
        private String mMiddle;
        private String mPrefix;
        private String mSuffix;

        private String mDisplayName;

        private String mPhoneticFamily;
        private String mPhoneticGiven;
        private String mPhoneticMiddle;

        private long mDataId;
        private long mRawContactId;
        private long mModifyTime;
        private int mDeleted;
        private String mModifyKey;
        private String mSyncKey;

        public NameData(long dataId, long rawContactId, String displayName, String given, String family,
                        String prefix, String middle, String suffix, String phoneticGiven, String phoneticMiddle,
                        String phoneticFamily, long modifyTime, int deleted, String modifyKey, String syncKey) {
            mDataId = dataId;
            mRawContactId = rawContactId;
            mDisplayName = displayName;
            mGiven = given;
            mFamily = family;
            mPrefix = prefix;
            mMiddle = middle;
            mSuffix = suffix;
            mPhoneticGiven = phoneticGiven;
            mPhoneticMiddle = phoneticMiddle;
            mPhoneticFamily = phoneticFamily;
            mModifyTime = modifyTime;
            mDeleted = deleted;
            mModifyKey = modifyKey;
            mSyncKey = syncKey;
        }

        @Override
        public ContentProviderOperation.Builder constructDeleteOperation() {
            return ContentProviderOperation
                    .newDelete(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());
        }

        @Override
        public ContentProviderOperation.Builder constructUpdateOpertation(MzSyncDataEntry mainEntry) {
            NameData nameData = (NameData) mainEntry;
            if (!TextUtils.equals(nameData.mDisplayName, mDisplayName)) {
                return null;
            }

            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newUpdate(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());

            if (!TextUtils.isEmpty(nameData.mGiven)) {
                builder.withValue(StructuredName.GIVEN_NAME, nameData.mGiven);
            }
            if (!TextUtils.isEmpty(nameData.mFamily)) {
                builder.withValue(StructuredName.FAMILY_NAME, nameData.mFamily);
            }
            if (!TextUtils.isEmpty(nameData.mMiddle)) {
                builder.withValue(StructuredName.MIDDLE_NAME, nameData.mMiddle);
            }
            if (!TextUtils.isEmpty(nameData.mPrefix)) {
                builder.withValue(StructuredName.PREFIX, nameData.mPrefix);
            }
            if (!TextUtils.isEmpty(nameData.mSuffix)) {
                builder.withValue(StructuredName.SUFFIX, nameData.mSuffix);
            }

            if (!TextUtils.isEmpty(nameData.mPhoneticGiven)) {
                builder.withValue(StructuredName.PHONETIC_GIVEN_NAME, nameData.mPhoneticGiven);
            }
            if (!TextUtils.isEmpty(nameData.mPhoneticFamily)) {
                builder.withValue(StructuredName.PHONETIC_FAMILY_NAME, nameData.mPhoneticFamily);
            }
            if (!TextUtils.isEmpty(nameData.mPhoneticMiddle)) {
                builder.withValue(StructuredName.PHONETIC_MIDDLE_NAME, nameData.mPhoneticMiddle);
            }

            if (!TextUtils.isEmpty(nameData.mDisplayName)) {
                builder.withValue(StructuredName.DISPLAY_NAME, nameData.mDisplayName);
            }
            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructInsertOperation(int backReferenceIndex) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(Data.CONTENT_URI);
            builder.withValueBackReference(ContactsContract.CommonDataKinds.StructuredName.RAW_CONTACT_ID, backReferenceIndex);
            builder.withValue(Data.MIMETYPE, ContactsContract.CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE);

            if (!TextUtils.isEmpty(mGiven)) {
                builder.withValue(ContactsContract.CommonDataKinds.StructuredName.GIVEN_NAME, mGiven);
            }
            if (!TextUtils.isEmpty(mFamily)) {
                builder.withValue(ContactsContract.CommonDataKinds.StructuredName.FAMILY_NAME, mFamily);
            }
            if (!TextUtils.isEmpty(mMiddle)) {
                builder.withValue(ContactsContract.CommonDataKinds.StructuredName.MIDDLE_NAME, mMiddle);
            }
            if (!TextUtils.isEmpty(mPrefix)) {
                builder.withValue(ContactsContract.CommonDataKinds.StructuredName.PREFIX, mPrefix);
            }
            if (!TextUtils.isEmpty(mSuffix)) {
                builder.withValue(ContactsContract.CommonDataKinds.StructuredName.SUFFIX, mSuffix);
            }

            if (!TextUtils.isEmpty(mPhoneticGiven)) {
                builder.withValue(ContactsContract.CommonDataKinds.StructuredName.PHONETIC_GIVEN_NAME, mPhoneticGiven);
            }
            if (!TextUtils.isEmpty(mPhoneticFamily)) {
                builder.withValue(ContactsContract.CommonDataKinds.StructuredName.PHONETIC_FAMILY_NAME, mPhoneticFamily);
            }
            if (!TextUtils.isEmpty(mPhoneticMiddle)) {
                builder.withValue(ContactsContract.CommonDataKinds.StructuredName.PHONETIC_MIDDLE_NAME, mPhoneticMiddle);
            }

            if (!TextUtils.isEmpty(mDisplayName)) {
                builder.withValue(ContactsContract.CommonDataKinds.StructuredName.DISPLAY_NAME, mDisplayName);
            }

            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructDifferentInsertOperation(long rawContactId) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(Data.CONTENT_URI);
            builder.withValue(StructuredName.RAW_CONTACT_ID, rawContactId);
            builder.withValue(Data.MIMETYPE, StructuredName.CONTENT_ITEM_TYPE);

            if (!TextUtils.isEmpty(mGiven)) {
                builder.withValue(StructuredName.GIVEN_NAME, mGiven);
            }
            if (!TextUtils.isEmpty(mFamily)) {
                builder.withValue(StructuredName.FAMILY_NAME, mFamily);
            }
            if (!TextUtils.isEmpty(mMiddle)) {
                builder.withValue(StructuredName.MIDDLE_NAME, mMiddle);
            }
            if (!TextUtils.isEmpty(mPrefix)) {
                builder.withValue(StructuredName.PREFIX, mPrefix);
            }
            if (!TextUtils.isEmpty(mSuffix)) {
                builder.withValue(StructuredName.SUFFIX, mSuffix);
            }

            if (!TextUtils.isEmpty(mPhoneticGiven)) {
                builder.withValue(StructuredName.PHONETIC_GIVEN_NAME, mPhoneticGiven);
            }
            if (!TextUtils.isEmpty(mPhoneticFamily)) {
                builder.withValue(StructuredName.PHONETIC_FAMILY_NAME, mPhoneticFamily);
            }
            if (!TextUtils.isEmpty(mPhoneticMiddle)) {
                builder.withValue(StructuredName.PHONETIC_MIDDLE_NAME, mPhoneticMiddle);
            }

            if (!TextUtils.isEmpty(mDisplayName)) {
                builder.withValue(StructuredName.DISPLAY_NAME, mDisplayName);
            }

            return builder;
        }

        @Override
        public boolean isEmpty() {
            return (TextUtils.isEmpty(mFamily) && TextUtils.isEmpty(mMiddle)
                    && TextUtils.isEmpty(mGiven) && TextUtils.isEmpty(mPrefix)
                    && TextUtils.isEmpty(mSuffix) && TextUtils.isEmpty(mDisplayName)
                    && TextUtils.isEmpty(mPhoneticFamily) && TextUtils.isEmpty(mPhoneticMiddle)
                    && TextUtils.isEmpty(mPhoneticGiven));
        }

        @Override
        public String getSyncData() {
            if (!TextUtils.isEmpty(mSyncKey)) {
                return mSyncKey;
            } else {
                return mDisplayName;
            }
        }

        @Override
        public String getData1() {
            return mDisplayName;
        }

        @Override
        public long getRawContactId() {
            return mRawContactId;
        }

        @Override
        public String getModifyKey() {
            return mModifyKey;
        }

        @Override
        public boolean isDeleted() {
            return mDeleted == 1;
        }

        @Override
        public long getModifyTime() {
            return mModifyTime;
        }
    }

    public class PhoneData implements MzSyncDataEntry {
        private final String mNumber;
        private final int mType;
        private final String mLabel;
        private boolean mIsPrimary;
        private boolean mIsSuperPrimary;

        private long mDataId;
        private long mRawContactId;
        private long mModifyTime;
        private int mDeleted;
        private String mModifyKey;
        private String mSyncKey;

        public PhoneData(long dataId, long rawContactId, String number, int type, String label,
                         boolean isPrimary, boolean isSuperPrimary, long modifyTime, int deleted,
                         String modifyKey, String syncKey) {
            mDataId = dataId;
            mRawContactId = rawContactId;
            mNumber = number;
            mType = type;
            mLabel = label;
            mIsPrimary = isPrimary;
            mIsSuperPrimary = isSuperPrimary;
            mModifyTime = modifyTime;
            mDeleted = deleted;
            mModifyKey = modifyKey;
            mSyncKey = syncKey;
        }

        @Override
        public boolean isEmpty() {
            return TextUtils.isEmpty(mNumber);
        }

        @Override
        public ContentProviderOperation.Builder constructInsertOperation(int backReferenceIndex) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation.newInsert(Data.CONTENT_URI);
            builder.withValueBackReference(Phone.RAW_CONTACT_ID, backReferenceIndex);
            builder.withValue(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);

            builder.withValue(Phone.TYPE, mType);
            if (mType == Phone.TYPE_CUSTOM) {
                builder.withValue(Phone.LABEL, mLabel);
            }
            builder.withValue(Phone.NUMBER, mNumber);
            if (mIsPrimary) {
                builder.withValue(Phone.IS_PRIMARY, 1);
            }
            if (mIsSuperPrimary) {
                builder.withValue(Phone.IS_SUPER_PRIMARY, 1);
            }
            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructDifferentInsertOperation(long rawContactId) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(Data.CONTENT_URI);
            builder.withValue(Phone.RAW_CONTACT_ID, rawContactId);
            builder.withValue(Data.MIMETYPE, Phone.CONTENT_ITEM_TYPE);

            builder.withValue(Phone.TYPE, mType);
            if (mType == Phone.TYPE_CUSTOM) {
                builder.withValue(Phone.LABEL, mLabel);
            }
            builder.withValue(Phone.NUMBER, mNumber);
            if (mIsPrimary) {
                builder.withValue(Phone.IS_PRIMARY, 1);
            }
            if (mIsSuperPrimary) {
                builder.withValue(Phone.IS_SUPER_PRIMARY, 1);
            }

            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructUpdateOpertation(MzSyncDataEntry mainEntry) {
            return null;
        }

        @Override
        public ContentProviderOperation.Builder constructDeleteOperation() {
            return ContentProviderOperation
                    .newDelete(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());
        }

        @Override
        public String getSyncData() {
            if (!TextUtils.isEmpty(mSyncKey)) {
                return mSyncKey;
            } else {
                return mNumber;
            }
        }

        @Override
        public String getData1() {
            return mNumber;
        }

        @Override
        public long getRawContactId() {
            return mRawContactId;
        }

        @Override
        public String getModifyKey() {
            return mModifyKey;
        }

        @Override
        public boolean isDeleted() {
            return mDeleted == 1;
        }

        @Override
        public long getModifyTime() {
            return mModifyTime;
        }
    }

    public class OrganizationData implements MzSyncDataEntry {
        private long mDataId;
        private String mOrganizationName;
        private String mDepartmentName;
        private String mTitle;
        private final String mPhoneticName;
        private boolean mIsPrimary;
        private long mModifyTime;
        private int mDeleted;
        private String mModifyKey;
        private String mSyncKey;
        private long mRawContactId;

        public OrganizationData(long dataId, long rawContactId, final String organizationName,
                                final String departmentName, final String titleName,
                                final String phoneticName, final boolean isPrimary, long modifyTime,
                                int deleted, String modifyKey, String syncKey) {
            mDataId = dataId;
            mRawContactId = rawContactId;
            mOrganizationName = organizationName;
            mDepartmentName = departmentName;
            mTitle = titleName;
            mPhoneticName = phoneticName;
            mIsPrimary = isPrimary;
            mModifyTime = modifyTime;
            mDeleted = deleted;
            mModifyKey = modifyKey;
            mSyncKey = syncKey;
        }

        @Override
        public ContentProviderOperation.Builder constructInsertOperation(int backReferenceIndex) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(ContactsContract.Data.CONTENT_URI);
            builder.withValueBackReference(Organization.RAW_CONTACT_ID, backReferenceIndex);
            builder.withValue(Data.MIMETYPE, Organization.CONTENT_ITEM_TYPE);
            if (mOrganizationName != null) {
                builder.withValue(Organization.COMPANY, mOrganizationName);
            }
            if (mDepartmentName != null) {
                builder.withValue(Organization.DEPARTMENT, mDepartmentName);
            }
            if (mTitle != null) {
                builder.withValue(Organization.TITLE, mTitle);
            }
            if (mPhoneticName != null) {
                builder.withValue(Organization.PHONETIC_NAME, mPhoneticName);
            }
            if (mIsPrimary) {
                builder.withValue(Organization.IS_PRIMARY, 1);
            }

            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructDeleteOperation() {
            return ContentProviderOperation
                    .newDelete(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());
        }

        @Override
        public ContentProviderOperation.Builder constructUpdateOpertation(MzSyncDataEntry mainEntry) {
            OrganizationData organizationData = (OrganizationData) mainEntry;

            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newUpdate(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());

            if (organizationData.mOrganizationName != null) {
                builder.withValue(Organization.COMPANY, organizationData.mOrganizationName);
            }
            if (organizationData.mDepartmentName != null) {
                builder.withValue(Organization.DEPARTMENT, organizationData.mDepartmentName);
            }
            if (organizationData.mTitle != null) {
                builder.withValue(Organization.TITLE, organizationData.mTitle);
            }
            if (organizationData.mPhoneticName != null) {
                builder.withValue(Organization.PHONETIC_NAME, organizationData.mPhoneticName);
            }
            if (organizationData.mIsPrimary) {
                builder.withValue(Organization.IS_PRIMARY, 1);
            }

            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructDifferentInsertOperation(long rawContactId) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(Data.CONTENT_URI);
            builder.withValue(Organization.RAW_CONTACT_ID, rawContactId);
            builder.withValue(Data.MIMETYPE, Organization.CONTENT_ITEM_TYPE);
            if (mOrganizationName != null) {
                builder.withValue(Organization.COMPANY, mOrganizationName);
            }
            if (mDepartmentName != null) {
                builder.withValue(Organization.DEPARTMENT, mDepartmentName);
            }
            if (mTitle != null) {
                builder.withValue(Organization.TITLE, mTitle);
            }
            if (mPhoneticName != null) {
                builder.withValue(Organization.PHONETIC_NAME, mPhoneticName);
            }
            if (mIsPrimary) {
                builder.withValue(Organization.IS_PRIMARY, 1);
            }

            return builder;
        }

        @Override
        public String getSyncData() {
            if (!TextUtils.isEmpty(mSyncKey)) {
                return mSyncKey;
            } else {
                return mOrganizationName;
            }
        }

        @Override
        public String getData1() {
            return mOrganizationName;
        }

        @Override
        public long getRawContactId() {
            return mRawContactId;
        }

        @Override
        public String getModifyKey() {
            return mModifyKey;
        }

        @Override
        public boolean isDeleted() {
            return mDeleted == 1;
        }

        @Override
        public long getModifyTime() {
            return mModifyTime;
        }

        @Override
        public boolean isEmpty() {
            return TextUtils.isEmpty(mOrganizationName) && TextUtils.isEmpty(mDepartmentName)
                    && TextUtils.isEmpty(mTitle) && TextUtils.isEmpty(mPhoneticName);
        }
    }

    public class PostalData implements MzSyncDataEntry {
        private final String mPobox;
        private final String mAddressData;
        private final String mStreet;
        private final String mLocalty;
        private final String mRegion;
        private final String mPostalCode;
        private final String mCountry;
        private final int mType;
        private final String mLabel;
        private boolean mIsPrimary;
        private boolean mIsSuperPrimary;
        private long mDataId;
        private long mModifyTime;
        private int mDeleted;
        private String mModifyKey;
        private String mSyncKey;
        private long mRawContactId;

        public PostalData(long dataId, long rawContactId, String addressData, String street, String pobox, String localty,
                          String region, String postalCode, String country, String label,
                          int type, boolean isPrimary, boolean isSuperPrimary, long modifyTime,
                          int deleted, String modifyKey, String syncKey) {
            mDataId = dataId;
            mRawContactId = rawContactId;
            mAddressData = addressData;
            mStreet = street;
            mPobox = pobox;
            mLocalty = localty;
            mRegion = region;
            mPostalCode = postalCode;
            mCountry = country;
            mType = type;
            mLabel = label;
            mIsPrimary = isPrimary;
            mIsSuperPrimary = isSuperPrimary;
            mDeleted = deleted;
            mModifyKey = modifyKey;
            mSyncKey = syncKey;
        }

        @Override
        public String getSyncData() {
            if (!TextUtils.isEmpty(mSyncKey)) {
                return mSyncKey;
            } else {
                return mAddressData;
            }
        }

        @Override
        public String getData1() {
            return mAddressData;
        }

        @Override
        public long getRawContactId() {
            return mRawContactId;
        }

        @Override
        public String getModifyKey() {
            return mModifyKey;
        }

        @Override
        public boolean isDeleted() {
            return mDeleted == 1;
        }

        @Override
        public long getModifyTime() {
            return mModifyTime;
        }

        @Override
        public boolean isEmpty() {
            return (TextUtils.isEmpty(mPobox)
                    && TextUtils.isEmpty(mAddressData)
                    && TextUtils.isEmpty(mStreet)
                    && TextUtils.isEmpty(mLocalty)
                    && TextUtils.isEmpty(mRegion)
                    && TextUtils.isEmpty(mPostalCode)
                    && TextUtils.isEmpty(mCountry));
        }

        @Override
        public ContentProviderOperation.Builder constructDeleteOperation() {
            return ContentProviderOperation
                    .newDelete(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());
        }

        @Override
        public ContentProviderOperation.Builder constructUpdateOpertation(MzSyncDataEntry mainEntry) {
            PostalData postalData = (PostalData) mainEntry;

            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newUpdate(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());

            builder.withValue(ContactsContract.Data.MIMETYPE, StructuredPostal.CONTENT_ITEM_TYPE);

            builder.withValue(StructuredPostal.TYPE, postalData.mType);
            if (postalData.mType == StructuredPostal.TYPE_CUSTOM) {
                builder.withValue(StructuredPostal.LABEL, postalData.mLabel);
            }

            builder.withValue(StructuredPostal.FORMATTED_ADDRESS, postalData.mAddressData);
            builder.withValue(StructuredPostal.STREET, postalData.mStreet);
            builder.withValue(StructuredPostal.POBOX, postalData.mPobox);
            builder.withValue(StructuredPostal.CITY, postalData.mLocalty);
            builder.withValue(StructuredPostal.REGION, postalData.mRegion);
            builder.withValue(StructuredPostal.POSTCODE, postalData.mPostalCode);
            builder.withValue(StructuredPostal.COUNTRY, postalData.mCountry);

            if (postalData.mIsPrimary) {
                builder.withValue(Data.IS_PRIMARY, 1);
            }

            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructInsertOperation(int backReferenceIndex) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(ContactsContract.Data.CONTENT_URI);
            builder.withValueBackReference(StructuredPostal.RAW_CONTACT_ID, backReferenceIndex);
            builder.withValue(ContactsContract.Data.MIMETYPE, StructuredPostal.CONTENT_ITEM_TYPE);

            builder.withValue(StructuredPostal.TYPE, mType);
            if (mType == StructuredPostal.TYPE_CUSTOM) {
                builder.withValue(StructuredPostal.LABEL, mLabel);
            }

            builder.withValue(StructuredPostal.FORMATTED_ADDRESS, mAddressData);
            builder.withValue(StructuredPostal.STREET, mStreet);
            builder.withValue(StructuredPostal.POBOX, mPobox);
            builder.withValue(StructuredPostal.CITY, mLocalty);
            builder.withValue(StructuredPostal.REGION, mRegion);
            builder.withValue(StructuredPostal.POSTCODE, mPostalCode);
            builder.withValue(StructuredPostal.COUNTRY, mCountry);

            if (mIsPrimary) {
                builder.withValue(Data.IS_PRIMARY, 1);
            }

            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructDifferentInsertOperation(long rawContactId) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(Data.CONTENT_URI);
            builder.withValue(StructuredPostal.RAW_CONTACT_ID, rawContactId);
            builder.withValue(Data.MIMETYPE, StructuredPostal.CONTENT_ITEM_TYPE);

            builder.withValue(StructuredPostal.TYPE, mType);
            if (mType == StructuredPostal.TYPE_CUSTOM) {
                builder.withValue(StructuredPostal.LABEL, mLabel);
            }

            builder.withValue(StructuredPostal.FORMATTED_ADDRESS, mAddressData);
            builder.withValue(StructuredPostal.STREET, mStreet);
            builder.withValue(StructuredPostal.POBOX, mPobox);
            builder.withValue(StructuredPostal.CITY, mLocalty);
            builder.withValue(StructuredPostal.REGION, mRegion);
            builder.withValue(StructuredPostal.POSTCODE, mPostalCode);
            builder.withValue(StructuredPostal.COUNTRY, mCountry);

            if (mIsPrimary) {
                builder.withValue(ContactsContract.Data.IS_PRIMARY, 1);
            }
            return builder;
        }
    }

    public class EmailData implements MzSyncDataEntry {
        private final String mEmailAddress;
        private final int mType;
        private final String mLabel;
        private final boolean mIsPrimary;
        private final boolean mIsSuperPrimary;

        private long mDataId;
        private long mModifyTime;
        private int mDeleted;
        private String mModifyKey;
        private String mSyncKey;
        private long mRawContactId;


        public EmailData(long dataId, long rawContactId, String emailAddress, int type, String label,
                         boolean isPrimary, boolean isSuperPrimary, long modifyTime, int deleted,
                         String modifyKey, String syncKey) {
            mDataId = dataId;
            mRawContactId = rawContactId;
            mEmailAddress = emailAddress;
            mType = type;
            mLabel = label;
            mIsPrimary = isPrimary;
            mIsSuperPrimary = isSuperPrimary;
            mModifyTime = modifyTime;
            mDeleted = deleted;
            mModifyKey = modifyKey;
            mSyncKey = syncKey;
        }

        @Override
        public boolean isEmpty() {
            return TextUtils.isEmpty(mEmailAddress);
        }

        @Override
        public ContentProviderOperation.Builder constructDeleteOperation() {
            return ContentProviderOperation
                    .newDelete(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());
        }

        @Override
        public ContentProviderOperation.Builder constructUpdateOpertation(MzSyncDataEntry mainEntry) {
            EmailData emailData = (EmailData) mainEntry;

            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newUpdate(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());

            builder.withValue(Email.TYPE, emailData.mType);
            if (emailData.mType == Email.TYPE_CUSTOM) {
                builder.withValue(Email.LABEL, emailData.mLabel);
            }
            builder.withValue(Email.DATA, emailData.mEmailAddress);
            if (emailData.mIsPrimary) {
                builder.withValue(Data.IS_PRIMARY, 1);
            }

            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructInsertOperation(int backReferenceIndex) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(ContactsContract.Data.CONTENT_URI);
            builder.withValueBackReference(Email.RAW_CONTACT_ID, backReferenceIndex);
            builder.withValue(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);

            builder.withValue(Email.TYPE, mType);
            if (mType == Email.TYPE_CUSTOM) {
                builder.withValue(Email.LABEL, mLabel);
            }
            builder.withValue(Email.DATA, mEmailAddress);
            if (mIsPrimary) {
                builder.withValue(Data.IS_PRIMARY, 1);
            }

            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructDifferentInsertOperation(long rawContactId) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(ContactsContract.Data.CONTENT_URI);
            builder.withValue(Email.RAW_CONTACT_ID, rawContactId);
            builder.withValue(Data.MIMETYPE, Email.CONTENT_ITEM_TYPE);

            builder.withValue(Email.TYPE, mType);
            if (mType == Email.TYPE_CUSTOM) {
                builder.withValue(Email.LABEL, mLabel);
            }
            builder.withValue(Email.DATA, mEmailAddress);
            if (mIsPrimary) {
                builder.withValue(Data.IS_PRIMARY, 1);
            }

            return builder;
        }

        @Override
        public String getSyncData() {
            if (!TextUtils.isEmpty(mSyncKey)) {
                return mSyncKey;
            } else {
                return mEmailAddress;
            }
        }

        @Override
        public String getData1() {
            return mEmailAddress;
        }

        @Override
        public long getRawContactId() {
            return mRawContactId;
        }

        @Override
        public String getModifyKey() {
            return mModifyKey;
        }

        @Override
        public boolean isDeleted() {
            return mDeleted == 1;
        }

        @Override
        public long getModifyTime() {
            return mModifyTime;
        }
    }

    private class PhotoData implements MzSyncDataEntry {
        private long mDataId;
        private long mRawContactId;
        private final boolean mIsPrimary;
        private final byte[] mBytes;
        private final String mPhotoUri;

        public PhotoData(long dataId, long rawContactId, String photoUri, byte[] photoBytes,
                         boolean isPrimary) {
            mDataId = dataId;
            mRawContactId = rawContactId;
            mPhotoUri = photoUri;
            mBytes = photoBytes;
            mIsPrimary = isPrimary;
        }

        @Override
        public boolean isEmpty() {
            return mBytes == null || mBytes.length == 0;
        }

        @Override
        public ContentProviderOperation.Builder constructDeleteOperation() {
            return ContentProviderOperation
                    .newDelete(ContentUris.appendId(Data.CONTENT_URI.buildUpon(), mDataId).build());
        }

        @Override
        public ContentProviderOperation.Builder constructUpdateOpertation(MzSyncDataEntry mainEntry) {
            return null;
        }

        @Override
        public ContentProviderOperation.Builder constructInsertOperation(int backReferenceIndex) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(Data.CONTENT_URI);
            builder.withValueBackReference(Photo.RAW_CONTACT_ID, backReferenceIndex);
            builder.withValue(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE);
            builder.withValue(Photo.PHOTO, mBytes);
            if (mIsPrimary) {
                builder.withValue(Photo.IS_PRIMARY, 1);
            }

            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructDifferentInsertOperation(long rawContactId) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(Data.CONTENT_URI);
            builder.withValue(Photo.RAW_CONTACT_ID, rawContactId);
            builder.withValue(Data.MIMETYPE, Photo.CONTENT_ITEM_TYPE);
            builder.withValue(Photo.PHOTO, mBytes);
            if (mIsPrimary) {
                builder.withValue(Photo.IS_PRIMARY, 1);
            }

            return builder;
        }

        @Override
        public String getSyncData() {
            return null;
        }

        @Override
        public String getData1() {
            return null;
        }

        @Override
        public long getRawContactId() {
            return mRawContactId;
        }

        @Override
        public String getModifyKey() {
            return null;
        }

        @Override
        public boolean isDeleted() {
            return false;
        }

        @Override
        public long getModifyTime() {
            return 0;
        }
    }

    private class GroupData implements MzSyncDataEntry {

        private long mDataId;
        private long mRawContactId;
        private long mGroupId;
        private String mTitle;
        private String mAccountName;
        private String mAccountType;
        private String mDataSet;

        public GroupData(String title, long dataId, long rawContactId, long groupId, String accoutName,
                         String accountType, String dataSet) {
            mTitle = title;
            mDataId = dataId;
            mRawContactId = rawContactId;
            mGroupId = groupId;
            mAccountName = accoutName;
            mAccountType = accountType;
            mDataSet = dataSet;
        }

        @Override
        public ContentProviderOperation.Builder constructDeleteOperation() {
            return null;
        }

        @Override
        public ContentProviderOperation.Builder constructUpdateOpertation(MzSyncDataEntry mainEntry) {
            return null;
        }

        @Override
        public ContentProviderOperation.Builder constructInsertOperation(int backReferenceIndex) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(ContactsContract.Data.CONTENT_URI);
            builder.withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, backReferenceIndex);
            builder.withValue(Data.MIMETYPE, GroupMembership.CONTENT_ITEM_TYPE);
            builder.withValue(GroupMembership.GROUP_ROW_ID, mGroupId);

            return builder;
        }

        @Override
        public ContentProviderOperation.Builder constructDifferentInsertOperation(long rawContactId) {
            final ContentProviderOperation.Builder builder = ContentProviderOperation
                    .newInsert(ContactsContract.Data.CONTENT_URI);
            builder.withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId);
            builder.withValue(Data.MIMETYPE, GroupMembership.CONTENT_ITEM_TYPE);
            builder.withValue(GroupMembership.GROUP_ROW_ID, mGroupId);
            builder.withYieldAllowed(true);

            return builder;
        }

        public void setGroupId(long groupId) {
            mGroupId = groupId;
        }

        @Override
        public String getSyncData() {
            return null;
        }

        @Override
        public String getData1() {
            return String.valueOf(mGroupId);
        }

        @Override
        public long getRawContactId() {
            return mRawContactId;
        }

        @Override
        public String getModifyKey() {
            return null;
        }

        @Override
        public boolean isDeleted() {
            return false;
        }

        @Override
        public long getModifyTime() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return mGroupId < 1;
        }
    }

}
