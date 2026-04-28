import { Component, type ErrorInfo, type ReactNode } from "react";
import { AlertTriangle, RefreshCw, Trash2 } from "lucide-react";
import { safeClearAppStorage } from "@/lib/safe-storage";

interface Props {
  children: ReactNode;
}

interface State {
  error: Error | null;
}

export default class AppErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    console.error("[AppErrorBoundary]", error, errorInfo);
  }

  private reload = () => {
    window.location.reload();
  };

  private clearStorageAndReload = () => {
    safeClearAppStorage();
    window.location.reload();
  };

  render() {
    if (!this.state.error) return this.props.children;

    return (
      <div className="min-h-screen bg-background px-4 py-10 text-foreground">
        <div className="mx-auto flex min-h-[70vh] max-w-lg flex-col items-center justify-center text-center">
          <div className="mb-5 grid h-16 w-16 place-items-center rounded-3xl border border-amber-500/25 bg-amber-500/10 text-amber-500">
            <AlertTriangle className="h-8 w-8" />
          </div>

          <h1 className="text-2xl font-black tracking-tight">页面加载异常</h1>
          <p className="mt-3 text-sm leading-6 text-muted-foreground">
            检测到浏览器里可能存在旧版登录状态、Discord 验证信息或接口地址缓存。
            无痕模式/换浏览器正常时，通常就是本地缓存数据不兼容导致的。
          </p>

          <div className="mt-6 flex w-full flex-col gap-3 sm:flex-row">
            <button
              type="button"
              onClick={this.reload}
              className="inline-flex flex-1 items-center justify-center gap-2 rounded-2xl border border-border bg-card px-4 py-3 text-sm font-bold text-foreground transition hover:bg-muted"
            >
              <RefreshCw className="h-4 w-4" />
              重新加载
            </button>
            <button
              type="button"
              onClick={this.clearStorageAndReload}
              className="inline-flex flex-1 items-center justify-center gap-2 rounded-2xl bg-primary px-4 py-3 text-sm font-bold text-primary-foreground transition hover:bg-primary/90"
            >
              <Trash2 className="h-4 w-4" />
              清理缓存并刷新
            </button>
          </div>

          {import.meta.env.DEV && (
            <pre className="mt-6 max-h-40 w-full overflow-auto rounded-xl border border-border bg-muted/40 p-3 text-left text-xs text-muted-foreground">
              {this.state.error.message}
            </pre>
          )}
        </div>
      </div>
    );
  }
}
