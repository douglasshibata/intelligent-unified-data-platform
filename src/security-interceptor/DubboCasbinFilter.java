package com.enterprise.security.interceptor;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.casbin.jcasbin.main.Enforcer;

/**
 * DubboCasbinFilter
 *
 * An Apache Dubbo 3 RPC interceptor/filter that enforces zero-trust access control at the microservice boundary.
 * Extracts multi-tenant context, role metadata, and resource paths from RPC request headers,
 * and passes them into an embedded Casbin engine enforcing policy validations using the PERM model.
 */
@Activate(group = {CommonConstants.PROVIDER})
public class DubboCasbinFilter implements Filter {

    private final Enforcer enforcer;

    public DubboCasbinFilter() {
        // Initialize the Casbin Enforcer using the standard PERM configuration
        // In a real system, the policy rule-set would be backed by an adapter linked to ShardingSphere/PostgreSQL
        String modelPath = "intelligent-unified-data-platform/src/security-interceptor/casbin_model.conf";
        String policyPath = "intelligent-unified-data-platform/src/security-interceptor/casbin_policy.csv";
        this.enforcer = new Enforcer(modelPath, policyPath);
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 1. Retrieve the attachment context (metadata headers mapped via Dubbo context)
        String tenantId = invocation.getAttachment("tenant_id");
        String userId = invocation.getAttachment("user_id");
        String userRole = invocation.getAttachment("user_role");

        // Retrieve target invocation resource properties (ABAC metrics)
        String serviceName = invoker.getInterface().getName();
        String methodName = invocation.getMethodName();
        String resourcePath = serviceName + "/" + methodName;
        String actionType = "READ"; // Default mapping, can also check method prefixes (e.g., save -> WRITE)

        if (methodName.startsWith("save") || methodName.startsWith("create") || methodName.startsWith("update") || methodName.startsWith("delete")) {
            actionType = "WRITE";
        }

        // 2. Fallback validation checks for identity properties
        if (tenantId == null || userRole == null) {
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION, "Access Denied: Missing multi-tenancy context headers.");
        }

        // 3. Enforce access rules using Casbin model: enforcer.enforce(subject, tenant/domain, resource, action)
        boolean authorized = enforcer.enforce(userRole, tenantId, resourcePath, actionType);

        if (!authorized) {
            System.err.println(String.format(
                "[CASBIN BLOCKED] Tenant: %s, User: %s (%s) tried to access %s [%s] but authorization was REJECTED.",
                tenantId, userId, userRole, resourcePath, actionType
            ));
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION, "Security Violation: Casbin authorization check failed.");
        }

        System.out.println(String.format(
            "[CASBIN APPROVED] Tenant: %s, User: %s (%s) successfully accessed %s [%s].",
            tenantId, userId, userRole, resourcePath, actionType
        ));

        // 4. Access authorized; proceed along the Dubbo filter chain
        return invoker.invoke(invocation);
    }
}
