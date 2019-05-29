package com.haima.sage.bigdata.etl.utils;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;

public class IOUtils {

	public static void close(Closeable... in) {
		if (in != null) {
			for (Closeable i : in) {
				try {
					i.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void close(boolean isClose, Closeable in) {
		if (isClose) {
			close(in);
		}
	}

	public static void close(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static boolean copy(InputStream in, OutputStream out, int bufSize,
			boolean close) throws IOException {
		BufferedInputStream bin = new BufferedInputStream(in);
		byte[] buf = new byte[bufSize];
		int offset = 0;
		while ((offset = bin.read(buf)) != -1) {
			out.write(buf, 0, offset);
		}
		out.flush();
		if (close) {
			IOUtils.close(in);
			IOUtils.close(out);
		}
		return true;
	}

	public static boolean copy(InputStream in, OutputStream out, boolean close)
			throws IOException {
		return copy(in, out, 4096, close);
	}

	public static boolean copy(InputStream in, OutputStream out, int bufSize)
			throws IOException {
		return copy(in, out, bufSize, false);
	}

	public static boolean copy(InputStream in, OutputStream out)
			throws IOException {
		return copy(in, out, 4096, false);
	}

	public static StringBuffer read(InputStream in) throws IOException {
		return read(in, false);
	}

	public static StringBuffer read(InputStream in, boolean close)
			throws IOException {
		return read(in, close, null);
	}

	public static StringBuffer read(InputStream in, StringBuffer res)
			throws IOException {
		return read(in, false, res);
	}

	public static StringBuffer read(InputStream in, boolean close,
			StringBuffer res) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		StringBuffer sb = res;
		if (sb == null) {
			sb = new StringBuffer();
		}
		int len = 0;
		char[] buf = new char[1024];
		while ((len = br.read(buf)) != -1) {
			sb.append(buf, 0, len);
		}
		IOUtils.close(close, br);
		return sb;
	}

	public static byte[] readBin(InputStream in) throws IOException {
		return readBin(in, false);
	}

	public static byte[] readBin(InputStream in, boolean close)
			throws IOException {
		try {
			byte[] bs = new byte[in.available()];
			int needs = in.available();
			int offset = 0, len;
			while (needs > 0) {
				len = in.read(bs, offset, bs.length - offset);
				offset += len;
				needs -= offset;
			}
			return bs;
		} finally {
			if (close) {
				IOUtils.close(in);
			}
		}
	}
}
