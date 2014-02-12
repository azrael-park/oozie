package org.apache.oozie.action.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.service.AuthorizationException;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HiveAccessService;
import org.apache.oozie.service.Services;

public class CustomHivePasswdProviderImpl implements HivePasswdProvider{

    private static final Log LOG = LogFactory.getLog(CustomHivePasswdProviderImpl.class);

    Class<? extends HivePasswdProvider> customHandlerClass;
    HivePasswdProvider customProvider;

    @SuppressWarnings("unchecked")
    CustomHivePasswdProviderImpl () {
        Configuration conf = Services.get().get(ConfigurationService.class).getConf();
        this.customHandlerClass = (Class<? extends HivePasswdProvider>)
                conf.getClass(HiveAccessService.CONF_PASSWD_PROVIDER_CLASS, HivePasswdProvider.class);
        this.customProvider = ReflectionUtils.newInstance(this.customHandlerClass, conf);
    }

    @Override
    public String getPassword(String user) throws AuthorizationException {
        return customProvider.getPassword(user);
    }
}
