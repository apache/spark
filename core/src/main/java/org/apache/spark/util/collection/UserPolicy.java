package org.apache.spark.util.collection;

import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;

/**
 * Created by h00251609 on 2014/11/29.
 */
public class UserPolicy extends Policy {
    private final Policy defaultPolicy;

    public UserPolicy() {
        super();
        defaultPolicy = Policy.getPolicy();
    }


    @Override
    public boolean implies(ProtectionDomain domain, Permission permission) {
        if (permission instanceof javax.management.MBeanTrustPermission) {
            return true;
        } else {
            return defaultPolicy.implies(domain, permission);
        }
    }

}
