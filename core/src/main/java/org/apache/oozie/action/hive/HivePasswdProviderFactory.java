package org.apache.oozie.action.hive;

import org.apache.oozie.service.AuthorizationException;

public class HivePasswdProviderFactory {

    public static enum ProviderMethods {
        CUSTOM("CUSTOM"),
        NONE("NONE");

        String providerMethod;

        ProviderMethods(String providerMethod) {
            this.providerMethod = providerMethod;
        }

    }

    public static HivePasswdProvider getHivePasswdProvider(String providerMethod) throws AuthorizationException{
        if (providerMethod.equals(ProviderMethods.CUSTOM.name())) {
            return new CustomHivePasswdProviderImpl();
        } else {
            return new AnonymousHivePasswdProvider();
        }
    }
}
