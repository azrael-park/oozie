package org.apache.oozie.action.hive;

import org.apache.oozie.service.AuthorizationException;

public class AnonymousHivePasswdProvider implements HivePasswdProvider {

    public AnonymousHivePasswdProvider () {

    }

    @Override
    public String getPassword(String user) throws AuthorizationException {
        return "";
    }
}
