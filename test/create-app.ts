import { DIContext } from "awilixify";

export function createAppFixture() {
	const apps = new Set<ReturnType<typeof DIContext.create>>();

	function createApp(
		config: Omit<Parameters<typeof DIContext.create>[0], "name"> & {
			name?: string;
		},
	) {
		const app = DIContext.create({ name: "Test", ...config });
		const close = app.close.bind(app);
		app.close = () => {
			apps.delete(app);
			return close();
		};
		apps.add(app);
		return app;
	}

	async function closeApps() {
		await Promise.all([...apps].map((app) => app.close()));
	}

	return { createApp, closeApps };
}
