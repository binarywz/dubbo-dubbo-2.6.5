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
package com.alibaba.dubbo.remoting.exchange.support.header;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.Transporters;
import com.alibaba.dubbo.remoting.exchange.ExchangeClient;
import com.alibaba.dubbo.remoting.exchange.ExchangeHandler;
import com.alibaba.dubbo.remoting.exchange.ExchangeServer;
import com.alibaba.dubbo.remoting.exchange.Exchanger;
import com.alibaba.dubbo.remoting.transport.DecodeHandler;

/**
 * DefaultMessenger
 *
 *
 */
public class HeaderExchanger implements Exchanger {

    public static final String NAME = "header";

    /**
     * service-refer-trace-4-6-25
     * @param url
     * @param handler
     * @return
     * @throws RemotingException
     */
    @Override
    public ExchangeClient connect(URL url, ExchangeHandler handler) throws RemotingException {
        /**
         * 创建HeaderExchangeClient实例，该方法包含了多个步骤，分别如下:
         * 1.创建HeaderExchangeHandler对象: new HeaderExchangeHandler(handler)
         * 2.创建DecodeHandler对象: new DecodeHandler(new HeaderExchangeHandler(handler))
         * 3.通过Transporters构建Client对象: Transporters.connect(url, new DecodeHandler(new HeaderExchangeHandler(handler)))
         * 4.创建HeaderExchangeClient对象
         */
        return new HeaderExchangeClient(Transporters.connect(url, new DecodeHandler(new HeaderExchangeHandler(handler))), true);
    }

    /**
     * service-export-trace-9-1
     * @param url
     * @param handler
     * @return
     * @throws RemotingException
     */
    @Override
    public ExchangeServer bind(URL url, ExchangeHandler handler) throws RemotingException {
        /**
         * 创建HeaderExchangeServer实例，该方法包含了多个逻辑，分别如下:
         * 1.new HeaderExchangerHandler(handler)
         * 2.new DecodeHandler(new HeaderExchangerHandler(handler))
         * 3.Transporters.bind(url, new DecodeHandler(new HeaderExchangeHandler(handler)))
         */
        return new HeaderExchangeServer(Transporters.bind(url, new DecodeHandler(new HeaderExchangeHandler(handler))));
    }

}
