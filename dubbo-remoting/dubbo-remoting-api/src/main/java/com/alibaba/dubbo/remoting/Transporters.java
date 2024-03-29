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
package com.alibaba.dubbo.remoting;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.remoting.transport.ChannelHandlerAdapter;
import com.alibaba.dubbo.remoting.transport.ChannelHandlerDispatcher;

/**
 * Transporter facade. (API, Static, ThreadSafe)
 */
public class Transporters {

    static {
        // check duplicate jar package
        Version.checkDuplicate(Transporters.class);
        Version.checkDuplicate(RemotingException.class);
    }

    private Transporters() {
    }

    public static Server bind(String url, ChannelHandler... handler) throws RemotingException {
        return bind(URL.valueOf(url), handler);
    }

    /**
     * service-export-trace-9-2
     * 服务提供者导出服务
     * @param url
     * @param handlers
     * @return
     * @throws RemotingException
     */
    public static Server bind(URL url, ChannelHandler... handlers) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handlers == null || handlers.length == 0) {
            throw new IllegalArgumentException("handlers == null");
        }
        ChannelHandler handler;
        if (handlers.length == 1) {
            handler = handlers[0];
        } else {
            /**
             * 若handlers元素数量大于1，则创建ChannelHandler分发器
             */
            handler = new ChannelHandlerDispatcher(handlers);
        }
        /**
         * service-export-trace-9-3
         * 获取自适应Transporter实例，并调用实例方法
         * getTransporter()方法获取的Transporter是在运行时创建的，类名为Transporter$Adaptive，
         * 即自适应扩展类。Transporter$Adaptive会在运行时根据传入的URL参数决定加载什么类型的Transporter，
         * 默认为NettyTransporter
         */
        return getTransporter().bind(url, handler);
    }

    public static Client connect(String url, ChannelHandler... handler) throws RemotingException {
        return connect(URL.valueOf(url), handler);
    }

    /**
     * 服务消费者引用服务
     * @param url
     * @param handlers
     * @return
     * @throws RemotingException
     */
    public static Client connect(URL url, ChannelHandler... handlers) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        ChannelHandler handler;
        if (handlers == null || handlers.length == 0) {
            handler = new ChannelHandlerAdapter();
        } else if (handlers.length == 1) {
            handler = handlers[0];
        } else {
            // 如果数量大于1，则创建一个ChannelHandler分发器
            handler = new ChannelHandlerDispatcher(handlers);
        }
        /**
         * 获取自适应Transporter实例，并调用实例方法
         * getTransporter()方法获取的Transporter是在运行时创建的，类名为Transporter$Adaptive，
         * 即自适应扩展类。Transporter$Adaptive会在运行时根据传入的URL参数决定加载什么类型的Transporter，
         * 默认为NettyTransporter
         */
        return getTransporter().connect(url, handler);
    }

    public static Transporter getTransporter() {
        return ExtensionLoader.getExtensionLoader(Transporter.class).getAdaptiveExtension();
    }

}