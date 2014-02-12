package org.apache.oozie.action.hive;

import org.apache.oozie.service.AuthorizationException;

public interface HivePasswdProvider {

    public String getPassword(String user) throws AuthorizationException;
}
