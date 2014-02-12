package org.apache.oozie.action.hive;

import org.apache.oozie.service.AuthorizationException;

public class NdapHivePasswdProvider implements HivePasswdProvider {

    public NdapHivePasswdProvider() {

    }

    @Override
    public String getPassword(String user) throws AuthorizationException {
        return user+"-hive-passwd";
    }
}
