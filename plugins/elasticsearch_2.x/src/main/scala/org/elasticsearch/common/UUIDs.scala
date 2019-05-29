package org.elasticsearch.common

object UUIDs {
	val TIME_UUID_GENERATOR = new TimeBasedUUIDGenerator()
	def base64UUID(): String = TIME_UUID_GENERATOR.getBase64UUID
}
