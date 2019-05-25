/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.plugin.classloader;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkPluginClassLoader extends URLClassLoader {
    private static final Logger LOG = LoggerFactory.getLogger(SparkPluginClassLoader.class);

    private static volatile SparkPluginClassLoader me 	             = null;
    private static  MyClassLoader				componentClassLoader = null;

    public SparkPluginClassLoader(String pluginType, Class<?> pluginClass ) throws Exception {
        super(SparkPluginClassLoaderUtil.getInstance().getPluginFilesForServiceTypeAndPluginclass(pluginType, pluginClass), null);
        componentClassLoader = AccessController.doPrivileged(
                new PrivilegedAction<MyClassLoader>() {
                    public MyClassLoader run() {
                        return  new MyClassLoader(Thread.currentThread().getContextClassLoader());
                    }
                }
        );
    }

    public static SparkPluginClassLoader getInstance(final String pluginType, final Class<?> pluginClass ) throws Exception {
        SparkPluginClassLoader ret = me;
        if ( ret == null) {
            synchronized(SparkPluginClassLoader.class) {
                ret = me;
                if ( ret == null){
                    me = ret = AccessController.doPrivileged(
                            new PrivilegedExceptionAction<SparkPluginClassLoader>(){
                                public SparkPluginClassLoader run() throws Exception {
                                    return  new SparkPluginClassLoader(pluginType,pluginClass);
                                }
                            }
                    );
                }
            }
        }
        return ret;
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> SparkPluginClassLoader.findClass(" + name + ")");
        }

        Class<?> ret = null;

        try {
            // first we try to find a class inside the child classloader
            if(LOG.isDebugEnabled()) {
                LOG.debug("SparkPluginClassLoader.findClass(" + name + "): calling childClassLoader().findClass() ");
            }

            ret = super.findClass(name);
        } catch( Throwable e ) {
            // Use the Component ClassLoader findclass to load when childClassLoader fails to find
            if(LOG.isDebugEnabled()) {
                LOG.debug("SparkPluginClassLoader.findClass(" + name + "): calling componentClassLoader.findClass()");
            }

            MyClassLoader savedClassLoader = getComponentClassLoader();
            if (savedClassLoader != null) {
                ret = savedClassLoader.findClass(name);
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== SparkPluginClassLoader.findClass(" + name + "): " + ret);
        }
        return ret;
    }

    @Override
    public synchronized Class<?> loadClass(String name) throws ClassNotFoundException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> SparkPluginClassLoader.loadClass(" + name + ")" );
        }

        Class<?> ret = null;

        try {
            // first we try to load a class inside the child classloader
            if (LOG.isDebugEnabled()) {
                LOG.debug("SparkPluginClassLoader.loadClass(" + name + "): calling childClassLoader.findClass()");
            }
            ret = super.loadClass(name);
        } catch(Throwable e) {
            // Use the Component ClassLoader loadClass to load when childClassLoader fails to find
            if (LOG.isDebugEnabled()) {
                LOG.debug("SparkPluginClassLoader.loadClass(" + name + "): calling componentClassLoader.loadClass()");
            }

            MyClassLoader savedClassLoader = getComponentClassLoader();

            if(savedClassLoader != null) {
                ret = savedClassLoader.loadClass(name);
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== SparkPluginClassLoader.loadClass(" + name + "): " + ret);
        }

        return ret;
    }

    @Override
    public URL findResource(String name) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> SparkPluginClassLoader.findResource(" + name + ") ");
        }

        URL ret =  super.findResource(name);

        if (ret == null) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("SparkPluginClassLoader.findResource(" + name + "): calling componentClassLoader.getResources()");
            }

            MyClassLoader savedClassLoader = getComponentClassLoader();
            if (savedClassLoader != null) {
                ret = savedClassLoader.getResource(name);
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== SparkPluginClassLoader.findResource(" + name + "): " + ret);
        }

        return ret;
    }

    @Override
    public Enumeration<URL> findResources(String name) throws IOException {
        Enumeration<URL> ret = null;

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> SparkPluginClassLoader.findResources(" + name + ") ");
        }

        ret =  new MergeEnumeration(findResourcesUsingChildClassLoader(name),findResourcesUsingComponentClassLoader(name));

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== SparkPluginClassLoader.findResources(" + name + ") ");
        }

        return ret;
    }

    public Enumeration<URL> findResourcesUsingChildClassLoader(String name) {

        Enumeration<URL> ret = null;

        try {
            if(LOG.isDebugEnabled()) {
                LOG.debug("SparkPluginClassLoader.findResourcesUsingChildClassLoader(" + name + "): calling childClassLoader.findResources()");
            }

            ret =  super.findResources(name);

        } catch ( Throwable t) {
            //Ignore any exceptions. Null / Empty return is handle in following statements
            if(LOG.isDebugEnabled()) {
                LOG.debug("SparkPluginClassLoader.findResourcesUsingChildClassLoader(" + name + "): class not found in child. Falling back to componentClassLoader", t);
            }
        }
        return ret;
    }

    public Enumeration<URL> findResourcesUsingComponentClassLoader(String name) {

        Enumeration<URL> ret = null;

        try {

            if(LOG.isDebugEnabled()) {
                LOG.debug("SparkPluginClassLoader.findResourcesUsingComponentClassLoader(" + name + "): calling componentClassLoader.getResources()");
            }

            MyClassLoader savedClassLoader = getComponentClassLoader();

            if (savedClassLoader != null) {
                ret = savedClassLoader.getResources(name);
            }

            if(LOG.isDebugEnabled()) {
                LOG.debug("<== SparkPluginClassLoader.findResourcesUsingComponentClassLoader(" + name + "): " + ret);
            }
        } catch( Throwable t) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("SparkPluginClassLoader.findResourcesUsingComponentClassLoader(" + name + "): class not found in componentClassLoader.", t);
            }
        }

        return ret;
    }

    public void activate() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> SparkPluginClassLoader.activate()");
        }

        //componentClassLoader.set(new MyClassLoader(Thread.currentThread().getContextClassLoader()));

        Thread.currentThread().setContextClassLoader(this);

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== SparkPluginClassLoader.activate()");
        }
    }

    public void deactivate() {

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> SparkPluginClassLoader.deactivate()");
        }

        MyClassLoader savedClassLoader = getComponentClassLoader();

        if(savedClassLoader != null && savedClassLoader.getParent() != null) {
            Thread.currentThread().setContextClassLoader(savedClassLoader.getParent());
        } else {
            LOG.warn("SparkPluginClassLoader.deactivate() was not successful.Couldn't not get the saved componentClassLoader...");
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== SparkPluginClassLoader.deactivate()");
        }
    }

    private MyClassLoader getComponentClassLoader() {
        return  componentClassLoader;
        //return componentClassLoader.get();
    }

    static class  MyClassLoader extends ClassLoader {
        public MyClassLoader(ClassLoader realClassLoader) {
            super(realClassLoader);
        }

        @Override
        public Class<?> findClass(String name) throws ClassNotFoundException { //NOPMD
            return super.findClass(name);
        }
    }

    static class MergeEnumeration implements Enumeration<URL> { //NOPMD

        Enumeration<URL>  e1 = null;
        Enumeration<URL>  e2 = null;

        public MergeEnumeration(Enumeration<URL> e1, Enumeration<URL> e2 ) {
            this.e1 = e1;
            this.e2 = e2;
        }

        @Override
        public boolean hasMoreElements() {
            return ( (e1 != null && e1.hasMoreElements() ) || ( e2 != null && e2.hasMoreElements()) );
        }

        @Override
        public URL nextElement() {
            URL ret = null;
            if (e1 != null && e1.hasMoreElements())
                ret = e1.nextElement();
            else if ( e2 != null && e2.hasMoreElements() ) {
                ret = e2.nextElement();
            }
            return ret;
        }
    }
}
