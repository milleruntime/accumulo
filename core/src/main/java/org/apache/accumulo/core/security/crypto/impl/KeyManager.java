package org.apache.accumulo.core.security.crypto.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.accumulo.core.security.crypto.CryptoService.CryptoException;

public class KeyManager
{
	public static Key generateKey(SecureRandom sr, int size)
	{
		byte[] bytes = new byte[size];
		sr.nextBytes(bytes);
		return new SecretKeySpec(bytes, "AES");
	}
	
	public static Key unwrapKey(byte[] fek, Key kek)
	{
		Key result = null;
		try
		{
			Cipher c = Cipher.getInstance("AESWrap", "SunJCE");
			c.init(Cipher.UNWRAP_MODE, kek);
			result = c.unwrap(fek, "AES", Cipher.SECRET_KEY);
		}
		catch (InvalidKeyException | NoSuchAlgorithmException  | NoSuchProviderException | NoSuchPaddingException e)	
		{
			throw new CryptoException("Unable to unwrap file encryption key");
		}
		return result;
	}

	public static byte[] wrapKey(Key fek, Key kek)
	{
		byte[] result = null;
		try
		{
			Cipher c = Cipher.getInstance("AESWrap", "SunJCE");
			c.init(Cipher.WRAP_MODE, kek);
			result = c.wrap(fek);
		}
		catch (InvalidKeyException | NoSuchAlgorithmException  | NoSuchProviderException | NoSuchPaddingException | IllegalBlockSizeException e)	
		{
			throw new CryptoException("Unable to wrap file encryption key");
		}
		
		return result;
	}

	public static SecretKeySpec loadKek(String keyId)
	{
		URI uri;
		SecretKeySpec key = null;
		try
		{
			uri = new URI(keyId);
			key = new SecretKeySpec(Files.readAllBytes(Paths.get(uri.getPath())),
					"AES");
		}
		catch (URISyntaxException | IOException e)
		{
			throw new CryptoException("Unable to load key encryption key.");
		}

		return key;

	}
}
