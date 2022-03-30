import { Buffer } from "buffer";
import got from "got";

export class Util {
	// tslint:disable-next-line no-any
	public static async DownloadFile(url: string, options: any = {}): Promise<Buffer> {
		if (!options.method) {
			options.method = "GET";
		}
		options.url = url;
		return await got(options).buffer();
	}

	public static FirstLetterToUpperCase(str: string) {
		return str.charAt(0).toUpperCase() + str.slice(1);
	}

	public static async sleep(timeout: number): Promise<void> {
		return new Promise((resolve, reject) => {
			setTimeout(resolve, timeout);
		});
	}
}
