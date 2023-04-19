package eu.unicore.persist.util;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.google.gson.GsonBuilder;

/**
 * Allows to customize how GSON handles a field in a DAO class.
 * Instances of the specified handlers will be registered with the 
 * {@link GsonBuilder}
 * 
 * @see GsonBuilder#registerTypeAdapter(java.lang.reflect.Type, Object)
 * 
 * @author schuller
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface JSON {

	Class<? extends GSONConverter>[] customHandlers() default {};
	
}
