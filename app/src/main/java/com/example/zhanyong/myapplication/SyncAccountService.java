package com.android.contacts.account;

import android.app.Activity;
import android.app.IntentService;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.net.Uri;
import android.os.Binder;
import android.os.Bundle;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.provider.MzDialerProviderContract;
import android.util.Log;

import com.android.contacts.common.model.account.AccountWithDataSet;
import com.android.contacts.util.AccountUtils;
import com.meizu.contacts.common.util.GroupUtils;
import com.meizu.contacts.db.MzSyncManager;
import com.meizu.contacts.db.action.MzAction;
import com.meizu.contacts.db.aggregation.MzAggregationUtils;
import com.meizu.contacts.db.aking.AkingContract;
import com.meizu.contacts.db.aking.AkingContract.MzData;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by zhanyong on 17-12-5.
 */

public class SyncAccountService extends IntentService implements MzSyncManager.Listener  {

    public static final String TAG = "SyncAccountService";

    public static final String EXTRA_CALLBACK_FINISH_INTENT = "callbackFinishIntent";
    public static final String EXTRA_CALLBACK_FAIL_INTENT ="callbackFailIntent";

    public static final String EXTRA_IS_AUTO_SYNC = "is_auto_sync";

    public static final String ACTION_SYNC_ACCOUNTS = "syncAccountsAction";
    public static final String EXTRA_HAS_MERGE_DATA = "hasMergeData";

    public static final int SYNC_BEGIN = 1;
    public static final int SYNC_ING = 2;
    public static final int SYNC_END = 3;

    private MyBinder mBinder;
    private Handler mMainHandler;
    private List<AccountWithDataSet> syncAccounts = new ArrayList<>();
    private Intent mIntent;
    private boolean mIsSuccess = true;
    public static int mState = SYNC_BEGIN;
    private long startTime;
    private ArrayList<Long> mSyncContctIds = new ArrayList<>();
    private int mSyncActionCount;
    private boolean mIsAutoSync = true;
    private boolean mStartSync = true;

    private static final CopyOnWriteArrayList<SyncAccountService.Listener> sListeners =
            new CopyOnWriteArrayList<SyncAccountService.Listener>();

    public class MyBinder extends Binder {
        public SyncAccountService getService() {
            return SyncAccountService.this;
        }
    }

    @Override
    public void onCreate() {
        super.onCreate();
        mBinder = new SyncAccountService.MyBinder();
        mMainHandler = new Handler(Looper.getMainLooper());
        syncAccounts = AccountUtils.getSaveSyncAccounts(getApplicationContext());
    }

    @Override
    public IBinder onBind(Intent intent) {
        return mBinder;
    }

    public SyncAccountService() {
        super(TAG);
        setIntentRedelivery(true);
    }

    public interface Listener {
        public void onSyncCompleted(Intent callbackIntent);
    }

    public static void registerListener(SyncAccountService.Listener listener) {
        if (!(listener instanceof Activity)) {
            throw new ClassCastException("Only activities can be registered to"
                    + " receive callback from " + SyncAccountService.class.getName());
        }
        sListeners.add(0, listener);
    }

    public static void unregisterListener(SyncAccountService.Listener listener) {
        sListeners.remove(listener);
    }

    private void deliverCallback(final Intent callbackIntent) {
        mMainHandler.post(new Runnable() {
            @Override
            public void run() {
                deliverCallbackOnUiThread(callbackIntent);
            }
        });
    }

    private void deliverCallbackOnUiThread(final Intent callbackIntent) {
        for (Listener listener : sListeners) {
            if (callbackIntent.getComponent().equals(
                    ((Activity) listener).getIntent().getComponent())) {
                listener.onSyncCompleted(callbackIntent);
                return;
            }
        }
    }

    @Override
    protected void onHandleIntent(Intent intent) {
        if (intent == null) {
            Log.d(TAG, "onHandleIntent: could not handle null intent");
            return;
        }
        String action = intent.getAction();

        if (ACTION_SYNC_ACCOUNTS.equals(action)) {
            mIntent = intent;
            syncContactsInAccounts(intent);
        }
    }

    @Override
    public void onChange(Set<MzAction> actions) {
        if (actions != null) {
            int size = actions.size();
            if (size > 0) {
                mSyncActionCount = size;
            }

            if (mSyncActionCount > 0 && size == 0) {
                if (mState == SYNC_ING) {
                    Log.d(TAG, "SYNC_ING ---after merge same name start sync ");
                    startSyncContacts();
                    mSyncActionCount = 0;
                } else if (mState == SYNC_END) {
                    Log.d(TAG, "SYNC_END---");
                    finishCallBack();
                }
            }
        }
    }

    public static Intent createSyncAccountsIntent(Context context,
                                                  boolean isAutoSync,
                                                  Class<? extends Activity> callbackActivity,
                                                  String callbackAction,
                                                  String noOperationcallbackAction) {
        Intent serviceIntent = new Intent(context, SyncAccountService.class);
        serviceIntent.setAction(ACTION_SYNC_ACCOUNTS);
        serviceIntent.putExtra(EXTRA_IS_AUTO_SYNC, isAutoSync);

        if (callbackActivity != null) {
            Intent callbackIntent = new Intent(context, callbackActivity);
            callbackIntent.setAction(callbackAction);
            serviceIntent.putExtra(EXTRA_CALLBACK_FINISH_INTENT, callbackIntent);

            Intent noOperationcallbackIntent = new Intent(context, callbackActivity);
            noOperationcallbackIntent.setAction(noOperationcallbackAction);
            serviceIntent.putExtra(EXTRA_CALLBACK_FAIL_INTENT, noOperationcallbackIntent);
        }
        return serviceIntent;
    }

    public void syncContactsInAccounts(Intent intent) {
        mState = SYNC_BEGIN;
        mSyncContctIds.clear();
        startTime = System.currentTimeMillis();
        mIsAutoSync = intent.getBooleanExtra(EXTRA_IS_AUTO_SYNC, true);
        if (syncAccounts.size() < 2) {
            Log.d(TAG, "syncAccounts size is less than 2---:" + syncAccounts.size());
            return;
        }
        StringBuilder selection = new StringBuilder();
        List<String> selectionArgs = new ArrayList<String>();
        Uri dataUri = MzDialerProviderContract.ViewData.VIEW_DATA_URI;
        String[] projection = new String[]{
                AkingContract.MzData.CONTACT_ID,
        };

        for(AccountWithDataSet accountWithDataSet : syncAccounts) {
            String accountName = accountWithDataSet.name;
            String accountType = accountWithDataSet.type;
            String dataSet = accountWithDataSet.dataSet;
            GroupUtils.appendAccountQueryParameterToSelectionAndSelectionArgs(accountName, accountType,
                    dataSet, selection, selectionArgs, false);
        }
        if (selection.length() > 0) {
            selection.append(") GROUP BY (" + MzData.CONTACT_ID);
        }

        try (final Cursor cursor = getContentResolver().query(dataUri, projection,
                selection.toString(), selectionArgs.toArray(new String[0]), null)) {
            while (cursor.moveToNext()) {
                long contactId = cursor.getLong(0);
                if (contactId != 0) {
                    mSyncContctIds.add(contactId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        Log.d(TAG, "the count of contacts is: " + mSyncContctIds.size());
        if (mSyncContctIds.size() == 0) {
            changeSyncState(true, false);
            return;
        }

        //被动同步取消，主动開啓
        Log.d(TAG, "register--this-and-unregister--auto : " );

        MzSyncAccountManager.getInstance(this).unRegisterContentObserver();
        MzSyncManager.getInstance().addListener(this);

        //同名同号码合并
        mState = SYNC_ING;
        Bundle bundle = getContentResolver().call(MzDialerProviderContract.AUTHORITY_URI, MzAggregationUtils.METHOD_AUTO_ALL,
                null, null);
        if (bundle.containsKey(EXTRA_HAS_MERGE_DATA) && bundle.getBoolean(EXTRA_HAS_MERGE_DATA)) {
            Log.d(TAG, "Has same name and number contacts ---sync after local syncManager");
        } else {
            startSyncContacts();
        }
    }

    private void startSyncContacts() {
        if (mStartSync) {
            mStartSync = false;
            MzSyncContactsMethod syncContactsMethod = new MzSyncContactsMethod(
                    this, mSyncContctIds, syncAccounts);
            Log.d(TAG, "start sync");
            syncContactsMethod.startSync();
        }
    }

    public void changeSyncState(boolean success, boolean hasChange) {
        mState = SYNC_END;
        mIsSuccess = success;
        Log.d(TAG, " changeSyncState---hasChange: " + hasChange + " success: " + success);
        if (!hasChange) {
            finishCallBack();
        }
    }

    public void finishCallBack() {
        //被动同步开启，主动取消
        MzSyncManager.getInstance().removeListener(this);
        Log.d(TAG, " finishCallBack---finish: " + mIsSuccess);
        if (mIntent != null && !mIsAutoSync) {
            Intent callbackIntent;
            if (mIsSuccess) {
                callbackIntent = mIntent.getParcelableExtra(EXTRA_CALLBACK_FINISH_INTENT);
            } else {
                callbackIntent = mIntent.getParcelableExtra(EXTRA_CALLBACK_FAIL_INTENT);
            }
            deliverCallback(callbackIntent);
        }
    }
}
