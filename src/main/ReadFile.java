package main;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class ReadFile {

	private final Charset CHAR_SET = StandardCharsets.UTF_8;
	private final String LS = System.getProperty("line.separator");
	private boolean isBlock = false;

	public List<String> readFile(String filePath) throws IOException {
		List<String> lines = Files.readAllLines(Paths.get(filePath), CHAR_SET);
		return lines;
	}

	public List<String> readFileExceptCommend(String filePath) throws IOException {
		List<String> lines = Files.readAllLines(Paths.get(filePath), CHAR_SET);
		String line;
		isBlock = false;
		for (int i = 0; i < lines.size();) {
			line = splitCommend(lines.get(i));
			if (isBlock || line == null || line.isEmpty()) {
				lines.remove(i);
				continue;
			}
			i++;
		}
		return lines;
	}

	public String toString(List<String> lines) throws IOException {
		StringBuilder stringBuilder = new StringBuilder();
		for (String line : lines) {
			stringBuilder.append(line);
			stringBuilder.append(LS);
		}
		return stringBuilder.toString();
	}

	public String splitCommend(String line) {
		String strArray[];
		if (!isBlock && line.contains("/*")) {
			isBlock = true;
			strArray = line.split("(/[*])");
			return strArray.length > 0 ? strArray[0] : null;
		}
		if (line.contains("*/")) {
			isBlock = false;
			strArray = line.split("([*]/)");
			return strArray.length > 0 ? strArray[1] : null;
		}
		if (line.contains("--")) {
			strArray = line.split("--");
			return strArray.length > 0 ? strArray[0] : null;
		}

		return line;
	}

	public String[] splitLine(String str) {
		return str.split("[\\s]*,[\\s]*");
	}

	public String[] splitLine(String str, String c) {
		return str.split("[\\s]*" + c + "[\\s]*");
	}

}
