package com.enterprise.iudp.security;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.AppResponse;

import org.casbin.jcasbin.main.Enforcer;

/**
 * Enterprise Apache Dubbo 3 Filter Interceptor.
 *
 * Enforces zero-trust multi-tenant authorization decisions directly on the inter-service RPC layer.
 * This interceptor is executed on the provider boundary prior to invoking any business logic,
 * utilizing an embedded Apache Casbin Enforcer instance loaded with PERM policies.
 */
@Activate(group = CommonConstants.PROVIDER, order = -10000)
public class DubboCasbinFilter implements Filter {

    private final Enforcer enforcer;

    public DubboCasbinFilter() {
        // Initialize the embedded JCasbin enforcer with model and policy configuration files
        String modelPath = "intelligent-unified-data-platform/casbin_model.conf";
        String policyPath = "intelligent-unified-data-platform/casbin_policy.csv";

        try {
            this.enforcer = new Enforcer(modelPath, policyPath);
            System.out.println("[DubboCasbinFilter] Embedded Casbin Enforcer initialized successfully.");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load Casbin configs inside Dubbo Interceptor: " + e.getMessage(), e);
        }
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 1. Extract context metadata propagated across service boundary via Dubbo's Attachment context
        String subject = RpcContext.getServiceContext().getAttachment("user_subject");
        String tenant = RpcContext.getServiceContext().getAttachment("tenant_id");
        String action = RpcContext.getServiceContext().getAttachment("action_verb");

        // Resolve target object/resource from the Dubbo invocation details (Interface + Method call)
        String serviceInterface = invoker.getInterface().getName();
        String methodName = invocation.getMethodName();
        String resourceObject = String.format("/dubbo/%s/%s", serviceInterface, methodName);

        // Fallback default parameters if attachments are empty to prevent malicious privilege escalation
        if (subject == null || subject.trim().isEmpty()) {
            subject = "anonymous_user";
        }
        if (tenant == null || tenant.trim().isEmpty()) {
            tenant = "anonymous_tenant";
        }
        if (action == null || action.trim().isEmpty()) {
            action = "execute"; // Default safe action constraint
        }

        System.out.printf("[DubboCasbinFilter] Security Intercepted. Subject: %s | Tenant: %s | Object: %s | Action: %s%n",
                subject, tenant, resourceObject, action);

        // 2. Perform high-performance, synchronous local Casbin policy enforcement check
        boolean authorized = false;
        try {
            authorized = enforcer.enforce(subject, tenant, resourceObject, action);
        } catch (Exception e) {
            System.err.println("[DubboCasbinFilter] Casbin Evaluation Error: " + e.getMessage());
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION, "Internal authorization check failed.");
        }

        // 3. Reject execution immediately if the security policy yields a negative outcome
        if (!authorized) {
            System.err.printf("[DubboCasbinFilter] Access DENIED for Subject %s on %s under Tenant %s.%n",
                    subject, resourceObject, tenant);
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION,
                    String.format("Security Violation: Subject '%s' is not authorized to execute '%s' on resource '%s' within Tenant '%s'.",
                            subject, action, resourceObject, tenant));
        }

        System.out.printf("[DubboCasbinFilter] Access GRANTED for Subject %s on %s.%n", subject, resourceObject);

        // 4. Authorized; delegate execution downstream to the target service provider
        return invoker.invoke(invocation);
    }
}
