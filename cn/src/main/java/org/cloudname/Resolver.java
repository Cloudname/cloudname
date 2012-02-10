package org.cloudname;

import sun.jvm.hotspot.tools.FinalizerInfo;

import java.util.List;

/**
 * This interface defines how we resolve endpoints in Cloudname.
 *
 * TODO(borud): we wish to make the resolving of addresses that
 *   contain a named strategy pluggable so that applications can add
 *   their own ranking and filtering strategy for endpoints.
 *
 *   It is not entirely clear how we want to do this yet, but one idea
 *   is to offer two main primitives: filtering and ranking.  The
 *   resolver starts by resolving all the possible matches to an
 *   address.  Then the filtering excludes endpoints from the resolved
 *   set.  The ranking orders endpoints according to some ranking
 *   criteria.
 *
 *   Note that both filtering and ranking can depend on properties
 *   that are completely outside CN.  For instance, you can do
 *   load-based ranking in which load-data is gathered by some
 *   external mechanism, but then acted on by the resolver.
 *
 *   Until we have a couple of use-cases, we will provide some default
 *   ranking and filtering strategies and see how those work out.
 *   Then we might think about what the API design should look like.
 *
 * @author borud
 */
public interface Resolver {

    /**
     * Resolve an address to a list of endpoints.  The order of the
     * endpoints may be subject to ranking criteria.
     *
     * @param address the address of the endpoint(s).
     * @throws CloudnameException if problems talking with storage.
     */
    public List<Endpoint> resolve(String address) throws CloudnameException;


    public interface ResolverFuture {
        void endpointModified(final Endpoint endpoint);
        void endpointDeleted(final Endpoint endpoint);
    }

    public void addResolverListener(String address, ResolverFuture future);

    public void shutdown();
}