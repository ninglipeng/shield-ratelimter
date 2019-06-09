package com.redis;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.DigestUtils;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisNoScriptException;

/**
 * lua脚本执行类
 * 
 */
@Component
public class LuaScriptUtils {

	@Autowired
	private RedisTemplate<String, Object> redisTemplate;

	private DefaultRedisScript<Long> getRedisScript;

	public void init() {
		getRedisScript = new DefaultRedisScript<>();
		getRedisScript.setResultType(Long.class);
		getRedisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource("rateLimter.lua")));
	}

	public void redisAddScriptExec() {
		init();
		/**
		 * 获取注解参数
		 */
		// 限流模块key
		String limitKey = "test";
		/**
		 * 执行Lua脚本
		 */
		List<String> keyList = new ArrayList<>();
		// 设置key值为注解中的值
		keyList.add(limitKey);

		List<String> args = new ArrayList<>();
		args.add("1.0");
		args.add("2.0");
		args.add("3.0");

		/**
		 * 调用脚本并执行
		 */
//		Long result = redisTemplate.execute(getRedisScript, keyList, 1.0, 2.0, 3.0);

		boolean result = exec(getRedisScript.getScriptAsString(), keyList, 1.0, 2.0, 3.0);

		System.out.println(result);

	}

	/**
	 * 命令行执行lua脚本
	 * 
	 * @param luaScript
	 * @param keys
	 * @param args
	 * @return
	 */
	public boolean exec(String luaScript, List<String> keys, List<String> args) {
		RedisCallback<Long> callback = (connection) -> {
			Object nativeConnection = connection.getNativeConnection();
			String luaScriptSha1 = DigestUtils.sha1DigestAsHex(luaScript);
			// 集群模式和单机模式虽然执行脚本的方法一样，但是没有共同的接口，所以只能分开执行
			// 集群模式
			if (nativeConnection instanceof JedisCluster) {
				@SuppressWarnings("resource")
				JedisCluster jedisCluster = (JedisCluster) nativeConnection;
				try {
					return (Long) jedisCluster.evalsha(luaScriptSha1, keys, args);
				} catch (Exception e) {
					if (!exceptionContainsNoScriptError(e)) {
						throw e instanceof RuntimeException ? (RuntimeException) e
								: new RedisSystemException(e.getMessage(), e);
					}
					return (Long) jedisCluster.eval(luaScript, keys, args);
				}
			}
			// 单机模式
			else if (nativeConnection instanceof Jedis) {
				@SuppressWarnings("resource")
				Jedis jedis = (Jedis) nativeConnection;
				try {
					return (Long) jedis.evalsha(luaScriptSha1, keys, args);
				} catch (Exception e) {
					if (!exceptionContainsNoScriptError(e)) {
						throw e instanceof RuntimeException ? (RuntimeException) e
								: new RedisSystemException(e.getMessage(), e);
					}
					return (Long) jedis.eval(luaScript, keys, args);
				}
			}
			return 0L;
		};
		Long result = redisTemplate.execute(callback);
		return result != null && result > 0;
	}

	/**
	 * 判断执行脚本抛出的异常
	 * 
	 * @param e
	 * @return
	 */
	private boolean exceptionContainsNoScriptError(Exception e) {
		if (e instanceof JedisNoScriptException) {
			String exMessage = e.getMessage();
			if (exMessage != null && exMessage.contains("NOSCRIPT")) {
				return true;
			}
		}

		if (!(e instanceof NonTransientDataAccessException)) {
			return false;
		}
		Throwable current = e;
		while (current != null) {
			String exMessage = current.getMessage();
			if (exMessage != null && exMessage.contains("NOSCRIPT")) {
				return true;
			}
			current = current.getCause();
		}
		return false;
	}

	/**
	 * 执行脚本带任意基本类型参数
	 * 
	 * @param luaScript
	 * @param keys
	 * @param args
	 * @return
	 */
	public boolean exec(String luaScript, List<String> keys, Object... args) {
		if (args != null && args.length > 0) {
			List<String> list = new ArrayList<>(args.length);
			for (Object a : args) {
				if(a!=null) {
					list.add(String.valueOf(a));
				}				
			}
			return exec(luaScript, keys, list);
		}
		return false;
	}

}
