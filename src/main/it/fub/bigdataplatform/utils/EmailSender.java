/**
 * Copyright 2017 Fondazione Ugo Bordoni
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.fub.bigdataplatform.utils;

import java.util.Properties;
import java.util.StringTokenizer;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.sun.mail.smtp.SMTPTransport;

/**
 * Permette l'invio di email. Utile per inviare allarmi.
 * 
 * EmailSender richiede un file di proprieta' in cui vengono valorizzati almeno:
 *
 * - alarm.email.mailServer (nome della macchina che ospita il mail server da usare)
 * - alarm.email.username (username dell'account da utilizzare per inviare le email)
 * - alarm.email.password (password dell'account da utilizzare per inviare le email)
 * - alarm.email.fromAddress (indirizzo di posta che verra' impostato nel campo "mittente")
 * - alarm.email.subject (oggetto assegnato alle email)
 * 
 * @author Marco Bianchi (mbianchi@fub.it)
 */

public class EmailSender {

	private static final String SMTP_PORT = "25"; // default port is 465 !

	private String mailServer;
	private String username;
	private String password;
	private String fromAddress;
	private String subject;
	private String propsPath;

	/**
	 * Crea un email sender a partire da un file di proprieta' cui il path e' specificato come parametro.
	 * 
	 * @param propertyPath
	 */
	public EmailSender(String propertyPath) {

		this.propsPath = propertyPath;

		Properties properties = Configuration.loadProps(propsPath);

		mailServer = properties.getProperty("alarm.email.mailServer");
		username = properties.getProperty("alarm.email.username");
		password = properties.getProperty("alarm.email.password");
		fromAddress = properties.getProperty("alarm.email.fromAddress");
		subject = properties.getProperty("alarm.email.subject");

	}

	/**
	 * Crea un email sender a partire dal file ./etc/alarm.properties
	 */
	public EmailSender() {

		this("../etc/alarm.properties");

	}
	
	/**
	 * Invia un messaggio di posta elettronica specificati nel file di configurazione mediante la proprieta':
	 * alarm.email.toRecipients
	 * E' possibile inserire piu' indirizzi di posta elettronica sono separati dal carattere 'virgola'.
	 * 
	 * @param message messaggio da inviare
	 */

	public void send(String message) {

		Properties properties = Configuration.loadProps(propsPath);

		boolean active = Boolean.parseBoolean(properties.getProperty(
				"alarm.email.enable", "false"));

		if (active) {

			String allRecipients = properties
					.getProperty("alarm.email.toRecipients");
			StringTokenizer tokenizer = new StringTokenizer(allRecipients, ",");
			String[] recipients = new String[tokenizer.countTokens()];
			int counter = 0;
			while (tokenizer.hasMoreTokens()) {
				recipients[counter++] = tokenizer.nextToken().trim();
			}

			try {
				sendMail(recipients, subject, message);
			} catch (Exception e) {
				return;
			}
		}

	}

	public void sendMail(String email, String subject, String text)
			throws Exception {
		String[] emailList = new String[1];
		emailList[0] = email;
		sendMail(emailList, subject, text);
	}

	public void sendMail(String[] emailList, String subject, String text)
			throws Exception {
		boolean debug = true;

		Properties props = new Properties();

		props.setProperty("mail.transport.protocol", "smtp");
		props.setProperty("mail.smtp.host", mailServer);
		props.setProperty("mail.smtp.auth", "true");
		props.setProperty("mail.debug", "true");
		props.setProperty("mail.smtp.user", username);
		props.setProperty("mail.smtp.password", password);
		props.setProperty("mail.smtp.port", SMTP_PORT);

		Authenticator auth = new SMTPAuthenticator(username, password);

		Session session = Session.getDefaultInstance(props, auth);

		session.setDebug(debug);

		Message msg = new MimeMessage(session);

		InternetAddress addressFrom = new InternetAddress(fromAddress);
		msg.setFrom(addressFrom);

		InternetAddress[] addressTo = new InternetAddress[emailList.length];

		for (int i = 0; i < emailList.length; i++) {
			addressTo[i] = new InternetAddress(emailList[i]);
		}

		msg.setRecipients(Message.RecipientType.TO, addressTo);
		msg.setSubject(subject);
		msg.setContent(text, "text/plain");

		SMTPTransport transport = (SMTPTransport) session.getTransport("smtp");
		transport.connect(mailServer, username, password);
		transport.sendMessage(msg, msg.getRecipients(Message.RecipientType.TO));

	}

	private class SMTPAuthenticator extends javax.mail.Authenticator {

		String username;
		String password;

		public SMTPAuthenticator(String username, String password) {
			this.username = username;
			this.password = password;
		}

		public PasswordAuthentication getPasswordAuthentication() {
			return new PasswordAuthentication(username, password);
		}
	}

	public static void main(String[] args) {
		new EmailSender("./etc/alarm.properties")
				.send("Test invio allarme da connettori.");
	}
}
