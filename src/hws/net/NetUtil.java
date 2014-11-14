package hws.net;

/**
 * @author Rodrigo Caetano O. ROCHA
 * @date 27 July 2013
 */

import java.net.NetworkInterface;
import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.SocketException;

import java.net.UnknownHostException;

import java.util.Enumeration;
/**
 * This class offers some useful static methods concerning network.
 */
public class NetUtil {

	// Suppress default constructor for noninstantiability
	private NetUtil() {
		throw new AssertionError();
	}

	/**
	 * Returns the IPv4 address of the local host. The returned address is not the loopback address.
	 * @return the IPv4 address of the local host reachable from other hosts in the same network.
	 */
	public static InetAddress getLocalInet4Address() throws SocketException {
		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
		while (interfaces.hasMoreElements()){
			NetworkInterface current = interfaces.nextElement();
			//System.out.println(current);
			if (!current.isUp() || current.isLoopback() || current.isVirtual()) continue;
			Enumeration<InetAddress> addresses = current.getInetAddresses();
			while (addresses.hasMoreElements()){
				InetAddress current_addr = addresses.nextElement();
				if (current_addr.isLoopbackAddress()) continue;
				if (current_addr instanceof Inet4Address) return current_addr;
			}
		}
		return null;
	}

	/**
	 * Returns the IPv6 address of the local host. The returned address is not the loopback address.
	 * @return the IPv6 address of the local host reachable from other hosts in the same network.
	 */
	public static InetAddress getLocalInet6Address() throws SocketException {
		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
		while (interfaces.hasMoreElements()){
			NetworkInterface current = interfaces.nextElement();
			//System.out.println(current);
			if (!current.isUp() || current.isLoopback() || current.isVirtual()) continue;
			Enumeration<InetAddress> addresses = current.getInetAddresses();
			while (addresses.hasMoreElements()){
				InetAddress current_addr = addresses.nextElement();
				if (current_addr.isLoopbackAddress()) continue;
				if (current_addr instanceof Inet6Address) return current_addr;
			}
		}
		return null;
	}

	public static String getLocalHostName() throws UnknownHostException {
		InetAddress localMachine = InetAddress.getLocalHost();
		return localMachine.getHostName();
	}

	public static String getLocalCanonicalHostName() throws UnknownHostException {
		InetAddress localMachine = InetAddress.getLocalHost();
		return localMachine.getCanonicalHostName();
	}
}
