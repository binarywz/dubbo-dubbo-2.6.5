/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.rpc.protocol.dubbo;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.AtomicPositiveInteger;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.TimeoutException;
import com.alibaba.dubbo.remoting.exchange.ExchangeClient;
import com.alibaba.dubbo.remoting.exchange.ResponseFuture;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.alibaba.dubbo.rpc.RpcResult;
import com.alibaba.dubbo.rpc.protocol.AbstractInvoker;
import com.alibaba.dubbo.rpc.support.RpcUtils;

import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DubboInvoker
 * Dubbo服务调用主要分为两个过程，分别是消费者发送请求和接收响应结果
 * 消费者端:
 * - 发送请求，服务接口的代理对象执行目标方法，被InvokerInvocationHandler#invoke方法拦截，经过路由过滤、负载均衡后选择一个DubboInvoker对象，
 *   调用doInvoke方法。创建一个Request对象，并生成全局唯一的请求ID，接着实例化一个DefaultFuture对象，将请求ID作为key，把DefaultFuture保存到
 *   一个ConcurrentHashMao中。最后，通过NettyClient把封装了目标方法信息的RpcInvocation序列化后发送出去
 * - 接收响应，通过响应ID，即请求ID，在缓存中找到对应的Future，执行doReceive方法。保存结果，接着唤醒对应的请求线程来处理响应结果
 *
 * 提供者端:
 * - NettyServer接收到请求后，根据协议得到信息并反序列化成对象，派发到线程池等待处理。信息会被封转成ChannelEventRunnable对象，类型为RECEIVED。
 *   工作线程最终会调用DubboProtocol#reply方法，根据port、path、version、group构建serviceKey，从缓存中找到对应Exporter，经过层层调用，
 *   最后会找到真正实现类，执行目标方法返回结果。
 *
 * 远程调用链
 * doInvoke:75, DubboInvoker (com.alibaba.dubbo.rpc.protocol.dubbo)
 * invoke:155, AbstractInvoker (com.alibaba.dubbo.rpc.protocol)
 * invoke:77, ListenerInvokerWrapper (com.alibaba.dubbo.rpc.listener)
 * invoke:75, MonitorFilter (com.alibaba.dubbo.monitor.support)
 * invoke:72, ProtocolFilterWrapper$1 (com.alibaba.dubbo.rpc.protocol)
 * invoke:54, FutureFilter (com.alibaba.dubbo.rpc.protocol.dubbo.filter)
 * invoke:72, ProtocolFilterWrapper$1 (com.alibaba.dubbo.rpc.protocol)
 * invoke:49, ConsumerContextFilter (com.alibaba.dubbo.rpc.filter)
 * invoke:72, ProtocolFilterWrapper$1 (com.alibaba.dubbo.rpc.protocol)
 * invoke:56, InvokerWrapper (com.alibaba.dubbo.rpc.protocol)
 * doInvoke:107, FailoverClusterInvoker (com.alibaba.dubbo.rpc.cluster.support)
 * invoke:293, AbstractClusterInvoker (com.alibaba.dubbo.rpc.cluster.support)
 * invoke:75, MockClusterInvoker (com.alibaba.dubbo.rpc.cluster.support.wrapper)
 * invoke:59, InvokerInvocationHandler (com.alibaba.dubbo.rpc.proxy)
 * sayHello:-1, proxy0 (com.alibaba.dubbo.common.bytecode)
 * main:35, Consumer (com.alibaba.dubbo.demo.consumer)
 *
 * proxy0#sayHello(String)
 *   —> InvokerInvocationHandler#invoke(Object, Method, Object[])
 *     —> MockClusterInvoker#invoke(Invocation)
 *       —> AbstractClusterInvoker#invoke(Invocation)
 *         —> FailoverClusterInvoker#doInvoke(Invocation, List<Invoker<T>>, LoadBalance)
 *           —> Filter#invoke(Invoker, Invocation)  // 包含多个 Filter 调用
 *             —> ListenerInvokerWrapper#invoke(Invocation)
 *               —> AbstractInvoker#invoke(Invocation)
 *                 —> DubboInvoker#doInvoke(Invocation)
 *                   —> ReferenceCountExchangeClient#request(Object, int)
 *                     —> HeaderExchangeClient#request(Object, int)
 *                       —> HeaderExchangeChannel#request(Object, int)
 *                         —> AbstractPeer#send(Object)
 *                           —> AbstractClient#send(Object, boolean)
 *                             —> NettyChannel#send(Object, boolean)
 *                               —> NioClientSocketChannel#write(Object)
 */
public class DubboInvoker<T> extends AbstractInvoker<T> {

    private final ExchangeClient[] clients;

    private final AtomicPositiveInteger index = new AtomicPositiveInteger();

    private final String version;

    private final ReentrantLock destroyLock = new ReentrantLock();

    private final Set<Invoker<?>> invokers;

    public DubboInvoker(Class<T> serviceType, URL url, ExchangeClient[] clients) {
        this(serviceType, url, clients, null);
    }

    public DubboInvoker(Class<T> serviceType, URL url, ExchangeClient[] clients, Set<Invoker<?>> invokers) {
        super(serviceType, url, new String[]{Constants.INTERFACE_KEY, Constants.GROUP_KEY, Constants.TOKEN_KEY, Constants.TIMEOUT_KEY});
        this.clients = clients;
        // get version.
        this.version = url.getParameter(Constants.VERSION_KEY, "0.0.0");
        this.invokers = invokers;
    }

    /**
     * service-invoke-trace-3-5-1
     * 远程调用过程
     * @param invocation
     * @return
     * @throws Throwable
     */
    @Override
    protected Result doInvoke(final Invocation invocation) throws Throwable {
        RpcInvocation inv = (RpcInvocation) invocation;
        final String methodName = RpcUtils.getMethodName(invocation);
        /**
         * 设置path和version到attachment中
         */
        inv.setAttachment(Constants.PATH_KEY, getUrl().getPath());
        inv.setAttachment(Constants.VERSION_KEY, version);

        ExchangeClient currentClient;
        if (clients.length == 1) {
            /**
             * 从数组中获取ExchangeClient
             */
            currentClient = clients[0];
        } else {
            currentClient = clients[index.getAndIncrement() % clients.length];
        }
        try {
            boolean isAsync = RpcUtils.isAsync(getUrl(), invocation);
            boolean isOneway = RpcUtils.isOneway(getUrl(), invocation);
            int timeout = getUrl().getMethodParameter(methodName, Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
            /**
             * 客户端发送请求
             * - 创建一个请求对象request
             * - 设置版本号和请求内容
             * - 构造Future对象并缓存
             * - 调用NettyClient发送请求
             * {@link com.alibaba.dubbo.remoting.exchange.support.header.HeaderExchangeChannel#request}
             *
             * 消费者接收响应结果
             * {@link com.alibaba.dubbo.remoting.exchange.support.DefaultFuture#received}
             */
            if (isOneway) {
                // oneWay方式发送，不管发送结果
                boolean isSent = getUrl().getMethodParameter(methodName, Constants.SENT_KEY, false);
                currentClient.send(inv, isSent);
                RpcContext.getContext().setFuture(null);
                return new RpcResult();
            } else if (isAsync) {
                // 异步发送，client发送请求后返回一个ResponseFuture，然后设置到上下文中，用户需要结果时，通过上下文获取future
                ResponseFuture future = currentClient.request(inv, timeout);
                RpcContext.getContext().setFuture(new FutureAdapter<Object>(future));
                return new RpcResult();
            } else {
                // 同步发送，直接调用future.get()阻塞等待结果
                // 异步和同步的区别是，future.get()是用户调用还是组件调用
                RpcContext.getContext().setFuture(null);
                return (Result) currentClient.request(inv, timeout).get();
            }
        } catch (TimeoutException e) {
            throw new RpcException(RpcException.TIMEOUT_EXCEPTION, "Invoke remote method timeout. method: " + invocation.getMethodName() + ", provider: " + getUrl() + ", cause: " + e.getMessage(), e);
        } catch (RemotingException e) {
            throw new RpcException(RpcException.NETWORK_EXCEPTION, "Failed to invoke remote method: " + invocation.getMethodName() + ", provider: " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isAvailable() {
        if (!super.isAvailable())
            return false;
        for (ExchangeClient client : clients) {
            if (client.isConnected() && !client.hasAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY)) {
                //cannot write == not Available ?
                return true;
            }
        }
        return false;
    }

    @Override
    public void destroy() {
        // in order to avoid closing a client multiple times, a counter is used in case of connection per jvm, every
        // time when client.close() is called, counter counts down once, and when counter reaches zero, client will be
        // closed.
        if (super.isDestroyed()) {
            return;
        } else {
            // double check to avoid dup close
            destroyLock.lock();
            try {
                if (super.isDestroyed()) {
                    return;
                }
                super.destroy();
                if (invokers != null) {
                    invokers.remove(this);
                }
                for (ExchangeClient client : clients) {
                    try {
                        client.close(ConfigUtils.getServerShutdownTimeout());
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }

            } finally {
                destroyLock.unlock();
            }
        }
    }
}
